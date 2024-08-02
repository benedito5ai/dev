extern crate chrono;
extern crate tokio;

use binance::api::*;
use binance::config::*;
use binance::market::*;
use binance::account::*;
use binance::general::General;
use binance::model::{KlineSummaries, KlineSummary};
use tokio::time::{self, Duration};
use std::sync::Arc;
use tokio::sync::Mutex;
use serde_json::json;
use serde::{Serialize};

#[derive(Serialize)]
struct Kline {
    timestamp: i64,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
}

#[tokio::main]
async fn main() {
    println!("Binance Bot Starting...");

    let api_key = "zDyiGjbpS6FlSPflPp8YyCPXZNXeFzNZx8VnYm1I3KmVI3u0kFQrBNmIJw9Rrme2";
    let secret_key = "6TvAEJTMUa3Ic1ld4tKcb5h9HUIVn4DJdWMB9CtSL6hmg5pSP0w7ZfrPUzxGlugy";

    let config = Config::default();
    let general = Arc::new(Mutex::new(General::new_with_config(Some(api_key.into()), Some(secret_key.into()), &config)));
    let market = Arc::new(Mutex::new(Market::new_with_config(Some(api_key.into()), Some(secret_key.into()), &config)));
    let account = Arc::new(Mutex::new(Account::new_with_config(Some(api_key.into()), Some(secret_key.into()), &config)));

    let symbol = "DOTUSDT";
    let target: f64 = 6.0; // % gain I want
    let max_loss: f64 = -4.0;
    let size: f64 = 1.0;
    let params = json!({"timeInForce": "GTC"});

    loop {
        let market_clone = Arc::clone(&market);
        let account_clone = Arc::clone(&account);
        let params_clone = params.clone();

        tokio::spawn(async move {
            bot(market_clone, account_clone, symbol, target, max_loss, size, &params_clone).await;
        });

        time::sleep(Duration::from_secs(30)).await;
    }
}

async fn ask_bid(market: Arc<Mutex<Market>>, symbol: &str) -> (f64, f64) {
    let market = market.lock().await;
    let ob = market.get_depth(symbol).unwrap();
    let bid = ob.bids[0].price;
    let ask = ob.asks[0].price;
    (ask, bid)
}

async fn daily_sma(market: Arc<Mutex<Market>>, symbol: &str) -> serde_json::Value {
    println!("starting indis...");
    let market = market.lock().await;
    let timeframe = "1d";
    let limit = 100;
    let klines = match market.get_klines(symbol, timeframe, limit, None, None).unwrap() {
        KlineSummaries::AllKlines(v) => v,
    };

    let mut df_d = json!({"timestamp": [], "open": [], "high": [], "low": [], "close": [], "volume": []});
    for kline in klines {
        df_d["timestamp"].as_array_mut().unwrap().push(json!(kline.open_time / 1000));
        df_d["open"].as_array_mut().unwrap().push(json!(kline.open));
        df_d["high"].as_array_mut().unwrap().push(json!(kline.high));
        df_d["low"].as_array_mut().unwrap().push(json!(kline.low));
        df_d["close"].as_array_mut().unwrap().push(json!(kline.close));
        df_d["volume"].as_array_mut().unwrap().push(json!(kline.volume));
    }

    // Calculate SMA
    let close_prices: Vec<f64> = df_d["close"].as_array().unwrap().iter().map(|x| x.as_f64().unwrap()).collect();
    let sma20_d: Vec<f64> = close_prices.windows(20).map(|window| window.iter().sum::<f64>() / 20.0).collect();
    df_d["sma20_d"] = json!(sma20_d);

    let bid = ask_bid(market.clone(), symbol).await.1;

    // if sma > bid = SELL, if sma < bid = BUY
    let sigs: Vec<&str> = sma20_d.iter().map(|&sma| if sma > bid { "SELL" } else { "BUY" }).collect();
    df_d["sig"] = json!(sigs);

    df_d
}

async fn f15_sma(market: Arc<Mutex<Market>>, symbol: &str) -> serde_json::Value {
    println!("starting 15 min sma...");
    let market = market.lock().await;
    let timeframe = "15m";
    let limit = 100;
    let klines = match market.get_klines(symbol, timeframe, limit, None, None).unwrap() {
        KlineSummaries::AllKlines(v) => v,
    };

    let mut df_f = json!({"timestamp": [], "open": [], "high": [], "low": [], "close": [], "volume": []});
    for kline in klines {
        df_f["timestamp"].as_array_mut().unwrap().push(json!(kline.open_time / 1000));
        df_f["open"].as_array_mut().unwrap().push(json!(kline.open));
        df_f["high"].as_array_mut().unwrap().push(json!(kline.high));
        df_f["low"].as_array_mut().unwrap().push(json!(kline.low));
        df_f["close"].as_array_mut().unwrap().push(json!(kline.close));
        df_f["volume"].as_array_mut().unwrap().push(json!(kline.volume));
    }

    // Calculate SMA
    let close_prices: Vec<f64> = df_f["close"].as_array().unwrap().iter().map(|x| x.as_f64().unwrap()).collect();
    let sma20_15: Vec<f64> = close_prices.windows(20).map(|window| window.iter().sum::<f64>() / 20.0).collect();
    df_f["sma20_15"] = json!(sma20_15);

    df_f["bp_1"] = json!(sma20_15.iter().map(|&x| x * 1.001).collect::<Vec<f64>>());
    df_f["bp_2"] = json!(sma20_15.iter().map(|&x| x * 0.997).collect::<Vec<f64>>());
    df_f["sp_1"] = json!(sma20_15.iter().map(|&x| x * 0.999).collect::<Vec<f64>>());
    df_f["sp_2"] = json!(sma20_15.iter().map(|&x| x * 1.003).collect::<Vec<f64>>());

    df_f
}

async fn get_df_vwap(market: Arc<Mutex<Market>>, symbol: &str) -> serde_json::Value {
    let market = market.lock().await;
    let timeframe = "1m";
    let limit = 100;
    let klines = match market.get_klines(symbol, timeframe, limit, None, None).unwrap() {
        KlineSummaries::AllKlines(v) => v,
    };

    let mut df_vwap = json!({"timestamp": [], "open": [], "high": [], "low": [], "close": [], "volume": []});
    for kline in klines {
        df_vwap["timestamp"].as_array_mut().unwrap().push(json!(kline.open_time / 1000));
        df_vwap["open"].as_array_mut().unwrap().push(json!(kline.open));
        df_vwap["high"].as_array_mut().unwrap().push(json!(kline.high));
        df_vwap["low"].as_array_mut().unwrap().push(json!(kline.low));
        df_vwap["close"].as_array_mut().unwrap().push(json!(kline.close));
        df_vwap["volume"].as_array_mut().unwrap().push(json!(kline.volume));
    }

    df_vwap
}

async fn vwap_indi(market: Arc<Mutex<Market>>, symbol: &str) -> serde_json::Value {
    println!("starting the vwap indicator to determine long or short...");
    let mut df_vwap = get_df_vwap(market.clone(), symbol).await;
    let close_prices: Vec<f64> = df_vwap["close"].as_array().unwrap().iter().map(|x| x.as_f64().unwrap()).collect();
    let volumes: Vec<f64> = df_vwap["volume"].as_array().unwrap().iter().map(|x| x.as_f64().unwrap()).collect();
    let mut vol_x_close: Vec<f64> = vec![0.0; close_prices.len()];
    let mut cum_vol: Vec<f64> = vec![0.0; close_prices.len()];
    let mut cum_vol_x_close: Vec<f64> = vec![0.0; close_prices.len()];

    for i in 0..close_prices.len() {
        vol_x_close[i] = close_prices[i] * volumes[i];
        if i == 0 {
            cum_vol[i] = volumes[i];
            cum_vol_x_close[i] = vol_x_close[i];
        } else {
            cum_vol[i] = cum_vol[i - 1] + volumes[i];
            cum_vol_x_close[i] = cum_vol_x_close[i - 1] + vol_x_close[i];
        }
    }

    let vwap: Vec<f64> = cum_vol_x_close.iter().zip(&cum_vol).map(|(&cvx, &cv)| cvx / cv).collect();
    df_vwap["vwap"] = json!(vwap);

    df_vwap
}

async fn bot(
    market: Arc<Mutex<Market>>,
    account: Arc<Mutex<Account>>,
    symbol: &str,
    target: f64,
    max_loss: f64,
    size: f64,
    params: &serde_json::Value,
) {
    println!("Starting bot...");
    let position = String::from("");
    let openpos_size = 0.0;
    let pos_size = openpos_size;

    let ask = ask_bid(market.clone(), symbol).await.0;
    let _bid = ask_bid(market.clone(), symbol).await.1;

    let df_d = daily_sma(market.clone(), symbol).await;
    let df_f = f15_sma(market.clone(), symbol).await;
    let df_vwap = vwap_indi(market.clone(), symbol).await;

    println!("Bot finished calculations.");
}