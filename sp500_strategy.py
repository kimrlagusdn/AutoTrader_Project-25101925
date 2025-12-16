import pandas as pd
import numpy as np
import warnings
import os

warnings.simplefilter(action='ignore', category=pd.errors.SettingWithCopyWarning)

# --- S&P 500 종목 목록 확보 ---

def get_sp500_symbols():
    """업로드된 CSV 파일에서 S&P 500 읽어옵니다."""
    csv_file = 'sp500_companies.csv'
    try:
        if not os.path.exists(csv_file):
            raise FileNotFoundError(f"'{csv_file}' 파일을 찾을 수 없습니다. 파일을 확인해 주세요.")

        df = pd.read_csv(csv_file)
        
        # CSV 컬럼 이름 탐색: 'Symbol' 또는 'Ticker'
        if 'Symbol' in df.columns:
            column_name = 'Symbol'
        elif 'Ticker' in df.columns:
             column_name = 'Ticker'
        else:
            raise ValueError(f"CSV 파일에 'Symbol' 또는 'Ticker' 컬럼이 없습니다. 현재 컬럼: {list(df.columns)}")
            
        symbols = df[column_name].tolist()
        
        valid_symbols = [s for s in symbols if isinstance(s, str) and '.' not in s and '-' not in s]
        
        print(f"S&P 500 심볼 (CSV 파일에서) {len(valid_symbols)}개 확보 완료.")
        return valid_symbols
        
    except Exception as e:
        print(f"오류: S&P 500 종목 목록을 가져오는 데 실패했습니다. 오류: {e}")
        # 오류 발생 시 최소한의 테스트를 위해 임시 목록 반환
        print("경고: 임시 대형주 목록 5개(AAPL, MSFT, GOOGL, AMZN, TSLA)로 대체합니다.")
        return ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']



# 매수 조건

def check_buy_condition(df):
    """
    매수 조건을 확인합니다: (오늘 종가 > 어제 종가)
    """
    if len(df) < 2: 
        return False
        
    latest = df.iloc[-1]
    previous = df.iloc[-2]

    if pd.isna(latest['close']) or pd.isna(previous['close']):
        return False
        
    if latest['close'] <= 0 or previous['close'] <= 0:
        return False

    is_up_today = latest['close'] > previous['close']

    return is_up_today