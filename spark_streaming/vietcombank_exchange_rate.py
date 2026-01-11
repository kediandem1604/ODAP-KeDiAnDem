#!/usr/bin/env python3
"""
Module lấy tỷ giá VNĐ từ Vietcombank
API ưu tiên, Selenium Chrome làm phương án dự phòng
Cache theo ngày (tỷ giá cập nhật mỗi ngày 1 lần)
"""

import json
import os
import requests
from datetime import datetime
from typing import Optional

# Selenium (phương án dự phòng)
try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from bs4 import BeautifulSoup
    SELENIUM_AVAILABLE = True
except Exception:
    SELENIUM_AVAILABLE = False

# Cấu hình
# Load config
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

# Cấu hình
API_URL = os.getenv("VCB_API_URL", "https://www.vietcombank.com.vn/api/exchangerates?date=")
WEB_URL = os.getenv("VCB_WEB_URL", "https://www.vietcombank.com.vn/vi-VN/KHCN/Cong-cu-Tien-ich/Ty-gia")
CACHE_FILE = "vcb_rate_cache.json"
DEFAULT_RATE = int(os.getenv("VCB_DEFAULT_RATE", 25000))

class VietcombankExchangeRate:
    """Lớp lấy tỷ giá từ Vietcombank"""
    
    def __init__(self, default_rate: int = DEFAULT_RATE):
        self.api_url = API_URL
        self.web_url = WEB_URL
        self.default_rate = default_rate
        self.cache_file = CACHE_FILE
    
    def _get_current_date(self) -> str:
        """Lấy ngày hiện tại (dd/mm/yyyy)"""
        return datetime.now().strftime("%d/%m/%Y")
    
    def _read_cache(self) -> Optional[float]:
        """Đọc cache nếu còn hiệu lực (cùng ngày)"""
        try:
            if not os.path.exists(self.cache_file):
                return None
            
            with open(self.cache_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            cached_date = data.get('date', '')
            current_date = self._get_current_date()
            
            # Kiểm tra có phải cùng ngày không
            if cached_date == current_date:
                rate = data.get('usd_sell')
                if rate:
                    print(f"Sử dụng cache ngày {cached_date}: {rate:,.0f} VNĐ")
                    return float(rate)
            else:
                print(f"Cache cũ (ngày {cached_date}), cần cập nhật")
            
            return None
        except:
            return None
    
    def _write_cache(self, rate: Optional[float]) -> None:
        """Ghi tỷ giá vào cache với ngày hiện tại"""
        try:
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump({
                    'date': self._get_current_date(),
                    'usd_sell': rate
                }, f)
        except:
            pass
    
    def get_usd_rate_from_api(self) -> Optional[float]:
        """Lấy tỷ giá USD từ API Vietcombank"""
        # Kiểm tra cache trước
        cached = self._read_cache()
        if cached is not None:
            return cached
        
        print("Đang gọi API Vietcombank...")
        try:
            response = requests.get(self.api_url, timeout=5)
            response.raise_for_status()
            
            data = response.json().get("Data", [])
            
            if not data:
                print("API: Không có dữ liệu")
                return None
            
            # Tìm USD
            for item in data:
                if item.get("currencyCode") == "USD":
                    sell_rate = float(item.get("sell", 0))
                    
                    if sell_rate > 0:
                        print(f"API thành công: {sell_rate:,.0f} VNĐ (ngày {self._get_current_date()})")
                        self._write_cache(sell_rate)
                        return sell_rate
            
            print("API: Không tìm thấy USD")
            return None
            
        except Exception as e:
            print(f"Lỗi API: {e}")
            return None
    
    def get_usd_rate_from_selenium(self) -> Optional[float]:
        """Lấy tỷ giá từ Selenium Chrome (phương án dự phòng)"""
        if not SELENIUM_AVAILABLE:
            print("Selenium không khả dụng")
            return None
        
        print("Đang khởi động Chrome...")
        
        driver = None
        try:
            # Cấu hình Chrome
            options = Options()
            options.add_argument('--headless')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            
            driver = webdriver.Chrome(options=options)
            driver.set_page_load_timeout(15)
            driver.get(self.web_url)
            
            # Đợi bảng xuất hiện
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'table'))
            )
            
            # Parse HTML
            soup = BeautifulSoup(driver.page_source, 'lxml')
            rows = soup.select('table tbody tr')
            
            # Tìm hàng USD
            for tr in rows:
                cols = [td.get_text(strip=True) for td in tr.select('td')]
                
                # Kiểm tra có USD không
                if any('USD' in c for c in cols):
                    # Lấy giá bán (cột cuối)
                    for c in reversed(cols):
                        try:
                            # Loại bỏ dấu phẩy và chấm
                            clean = c.replace(',', '').replace('.', '')
                            if clean.isdigit():
                                rate = float(clean)
                                if rate > 20000:  # Kiểm tra giá trị hợp lý
                                    print(f"Selenium thành công: {rate:,.0f} VNĐ")
                                    self._write_cache(rate)
                                    return rate
                        except:
                            continue
            
            print("Selenium: Không tìm thấy USD")
            return None
            
        except Exception as e:
            print(f"Lỗi Selenium: {e}")
            return None
        finally:
            if driver:
                try:
                    driver.quit()
                except:
                    pass
    
    def get_usd_to_vnd_rate(self) -> float:
        """
        Lấy tỷ giá USD sang VNĐ
        Ưu tiên: API (cache theo ngày) → Selenium → Tỷ giá mặc định
        """
        # Thử API với cache theo ngày
        rate = self.get_usd_rate_from_api()
        if rate is not None:
            return float(rate)
        
        # API thất bại, thử Selenium
        print("\nAPI thất bại, chuyển sang Selenium...")
        rate = self.get_usd_rate_from_selenium()
        if rate is not None:
            return float(rate)
        
        # Cả 2 thất bại, dùng tỷ giá mặc định
        print(f"\nSử dụng tỷ giá mặc định: {self.default_rate:,.0f} VNĐ")
        return float(self.default_rate)

# Test
if __name__ == "__main__":
    print("=== TEST MODULE TỶ GIÁ ===\n")
    
    vcb = VietcombankExchangeRate()
    rate = vcb.get_usd_to_vnd_rate()
    
    print(f"\n=== KẾT QUẢ ===")
    print(f"Tỷ giá: 1 USD = {rate:,.0f} VNĐ")
    
    # Test chuyển đổi
    usd = 100
    vnd = usd * rate
    print(f"Ví dụ: ${usd} USD = {vnd:,.0f} VNĐ")