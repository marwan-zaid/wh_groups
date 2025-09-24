import pandas as pd
import time
import random
import os
from datetime import datetime
import concurrent.futures
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from functools import lru_cache
from webdriver_manager.chrome import ChromeDriverManager

def create_chrome_options():
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--log-level=3")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
    return options

@lru_cache(maxsize=1000)
def get_group_name_cached(link):
    if pd.isna(link) or "chat.whatsapp.com" not in str(link):
        return link, "❌ رابط غير صالح"

    driver = None
    try:
        driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=create_chrome_options()
        )
        driver.set_page_load_timeout(15)

        # المحاولة الأولى: قراءة meta tag
        try:
            driver.get(str(link))
            meta = WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "meta[property='og:title']"))
            )
            if meta:
                name = meta.get_attribute("content").strip()[:100]
                if name:
                    time.sleep(random.uniform(0.5, 1.5))
                    return link, name
        except:
            pass

        # المحاولة الثانية: قراءة h3
        try:
            h3 = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "h3._9vd5"))
            )
            if h3:
                return link, h3.text.strip()[:100]
        except:
            pass

        return link, "⚠️ لم يتم العثور على الاسم"

    except Exception as e:
        return link, f"خطأ: {str(e)}"
    finally:
        time.sleep(random.uniform(1, 2))  # <-- تأخير هنا بين كل محاولة وأخرى
        if driver:
            driver.quit()

def load_previous_results():
    """تحميل النتائج السابقة من أحدث ملف مؤقت"""
    temp_files = [f for f in os.listdir() if f.startswith("temp_results_")]
    if not temp_files:
        return None, 0

    latest_file = max(temp_files, key=os.path.getctime)
    try:
        temp_df = pd.read_excel(latest_file)
        last_count = int(latest_file.split("_")[-2])
        return dict(zip(temp_df["whatsAppLink"], temp_df["Groups Name"])), last_count
    except:
        return None, 0

def main(start_from=0):
    file_path = "groups name.xlsx"
    try:
        df = pd.read_excel(file_path)
    except FileNotFoundError:
        print(f"❌ لم يتم العثور على ملف '{file_path}'.")
        return

    link_column = "whatsAppLink"
    if link_column not in df.columns:
        print(f"❌ العمود '{link_column}' غير موجود في ملف Excel.")
        return

    links_to_process = df[link_column].dropna().tolist()
    total = len(links_to_process)

    if total == 0:
        print("لم يتم العثور على روابط صالحة للمعالجة.")
        return

    MAX_WORKERS = 4   # عدد الثريدات
    CHUNK_SIZE = 500  # عدد الروابط قبل الحفظ المؤقت

    # تحميل النتائج السابقة إذا وجدت
    if start_from == 0:
        prev_results, prev_count = load_previous_results()
        if prev_results:
            results = prev_results
            start_from = prev_count
            print(f"⚡ تم تحميل {len(results)} نتيجة سابقة، الاستئناف من الرابط رقم {start_from+1}")
        else:
            results = {}
    else:
        results = {}

    print(f"🚀 بدء/استئناف العمل من الرابط رقم {start_from+1} من أصل {total}")
    print(f"⚙️ عدد الثريدات العاملية: {MAX_WORKERS}")
    print(f"💾 سيتم الحفظ المؤقت كل {CHUNK_SIZE} رابط\n")

    processed_count = start_from
    start_time = time.time()

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_link = {
            executor.submit(get_group_name_cached, link): link
            for link in links_to_process[start_from:]
        }

        for future in concurrent.futures.as_completed(future_to_link):
            original_link, result_name = future.result()
            results[original_link] = result_name
            processed_count += 1

            if processed_count % CHUNK_SIZE == 0 or processed_count == total:
                temp_df = df.copy()
                temp_df["Groups Name"] = temp_df[link_column].map(results)

                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                temp_filename = f"temp_results_{timestamp}_{processed_count}_of_{total}.xlsx"
                temp_df.to_excel(temp_filename, index=False)

                print(f"\n💾 تم حفظ النتائج المؤقتة: {temp_filename}")

            percent = (processed_count / total) * 100
            elapsed_time = time.time() - start_time
            avg_time = elapsed_time / (processed_count - start_from)
            remaining = total - processed_count
            eta = (remaining * avg_time) / 60

            print(
                f"\r🔍 {processed_count}/{total} ({percent:.1f}%) | ⏱️ {eta:.1f} دقيقة متبقية | آخر نتيجة: {result_name[:50]}",
                end="",
                flush=True,
            )

    df["Groups Name"] = df[link_column].map(results)
    final_filename = f"final_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    df.to_excel(final_filename, index=False)

    print(f"\n\n✅ تم الانتهاء بنجاح!")
    print(f"📊 النتائج النهائية محفوظة في: {final_filename}")
    print(f"⏱️ الوقت الإجمالي: {(time.time() - start_time) / 60:.2f} دقيقة")

if __name__ == "__main__":
    # ابدأ من البداية
    main(start_from=0)
