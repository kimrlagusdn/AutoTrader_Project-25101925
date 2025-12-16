import os
import time
from datetime import datetime, timedelta
import pandas as pd 

from dotenv import load_dotenv 


from sp500_strategy import get_sp500_symbols, calculate_indicators, check_buy_condition

from alpaca.data.requests import StockBarsRequest
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce

from alpaca.data.historical import StockHistoricalDataClient 
from alpaca.data.timeframe import TimeFrame 



load_dotenv()



def initialize_clients():
    """Alpaca API 클라이언트를 초기화하고 반환합니다."""
    try:
        trading_client = TradingClient(os.environ['APCA_API_KEY_ID'], 
                                       os.environ['APCA_API_SECRET_KEY'], 
                                       paper=True) 

        data_client = StockHistoricalDataClient(os.environ['APCA_API_KEY_ID'], 
                                                os.environ['APCA_API_SECRET_KEY']) 
                                                
        account = trading_client.get_account() 
        if account.status.value != 'ACTIVE':
            raise Exception(f"계좌 상태가 'ACTIVE'가 아닙니다: {account.status.value}")
                                                
        print("Alpaca API (Paper Trading) 연결 성공.")
        return trading_client, data_client
    except KeyError as e:
        print(f"오류: 환경 변수 설정이 필요합니다. {e}를 설정해주세요. (.env 파일 확인)")
        exit()
    except Exception as e:
        print(f"치명적인 오류: API 연결 및 인증 실패. 키/Secret 또는 .env 파일을 확인하세요. 오류: {e}")
        exit()


def run_sp500_auto_trader():
    """S&P 500 전 종목을 순회하며 매매 로직을 실행하는 메인 함수."""
    
    trading_client, data_client = initialize_clients()
    sp500_symbols = get_sp500_symbols() 
    
    print(f"\n--- S&P 500 자동 매매 실행: 총 {len(sp500_symbols)} 종목 대상 ---")
    
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=120) 
    
    
    for symbol in sp500_symbols:
        try:
            print(f"  > {symbol} 데이터 처리 및 조건 확인 중...")
            
            # 과거 일봉 데이터 요청 (Data API)
            bar_request = StockBarsRequest(
                symbol_or_symbols=[symbol],
                timeframe=TimeFrame.Day, 
                start=start_date,
                end=end_date
            )
            bars = data_client.get_stock_bars(bar_request)
            
            # DataFrame으로 변환 및 불필요한 열 제거
            bars_df = bars.df.reset_index(level=0).drop(columns=['symbol', 'trade_count', 'vwap'], errors='ignore')
            
            if bars_df.empty:
                print(f"    - {symbol}: 데이터 부족. 스킵.")
                continue

            # 지표 계산 및 매수 조건 확인
            bars_df = calculate_indicators(bars_df) 
            
            if check_buy_condition(bars_df): 
                print(f"  BUY SIGNAL: {symbol} - 매수 조건 만족!")
                
                # 매수 주문 실행
                order_data = MarketOrderRequest(
                    symbol=symbol,
                    qty=1, # 고정 1주 매수
                    side=OrderSide.BUY,
                    time_in_force=TimeInForce.GTC 
                )
                
                submit_order = trading_client.submit_order(order_data)
                print(f"    - 주문 성공: ID={submit_order.id}, 상태={submit_order.status}")
                
            else:
                print(f"  NO SIGNAL: {symbol} - 조건 불만족.")

        except Exception as e:
            # Alpaca에서 거래 불가능한 종목이거나 기타 API 요청 오류
            print(f"  오류 발생 ({symbol}): {e}")
        
        time.sleep(0.5)

    monitor_performance(trading_client)

# --- 수익률  ---

def monitor_performance(trading_client):
    """현재 계좌 상태와 포지션 수익률을 출력합니다."""
    print("\n" + "="*50)
    print("                최종 계좌 및 수익률 모니터링")
    print("="*50)
    
    try:
        account = trading_client.get_account()
        
        current_equity = float(account.equity)
        last_equity = float(account.last_equity) 
        
        # equity_day 및 change_today_percent 계산
        daily_pnl = current_equity - last_equity
        daily_pnl_percent = (daily_pnl / last_equity) * 100 if last_equity != 0 else 0
        
        print(f"계좌 상태: {account.status.value}")
        print(f"현재 총자산 가치 (Equity): ${current_equity:.2f}")
        print(f"일일 수익/손실 (Daily P&L): ${daily_pnl:.2f}") 
        print(f"P&L % (오늘): {daily_pnl_percent:.2f}%") 
        
        positions = trading_client.get_all_positions()
        if positions:
            print("\n--- 현재 보유 종목별 수익률 ---")
            for p in positions:
                pl_percent = float(p.unrealized_plpc) * 100
                print(f"  > {p.symbol}: 수량={p.qty}, 미실현 손익%={pl_percent:.2f}%")
        else:
            print("\n--- 현재 보유 포지션 없음 ---")
            
    except Exception as e:
        print(f"계좌 정보 조회 중 오류 발생: {e}")

if __name__ == "__main__":
    run_sp500_auto_trader()