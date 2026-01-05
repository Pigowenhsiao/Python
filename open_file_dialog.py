import re
import os
import logging
import time
from tkinter import Tk
from tkinter.filedialog import askopenfilename

import requests
from bs4 import BeautifulSoup

# 設定日誌
logging.basicConfig(
    filename='paper_research.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    encoding='utf-8'
)

def extract_urls_from_file():
    print("\n1/5: 正在初始化檔案選擇器...")
    # 隱藏 Tkinter 主視窗
    Tk().withdraw()
    
    print("2/5: 正在選擇檔案...")
    file_path = askopenfilename(filetypes=[("Text files", "*.txt")], title="選擇一個TXT檔案")
    
    if not file_path:
        print("3/5: 未選擇檔案，程式終止。")
        logging.info("未選擇檔案，程式終止。")
        return
    else:
        print("3/5: 檔案選擇完成，準備讀取內容...")
    
    try:
        print("3/5: 正在讀取檔案內容...")
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                content = file.read()
        except UnicodeDecodeError:
            with open(file_path, 'r', encoding='gbk') as file:
                content = file.read()
        
        # 逐行解析檔案，取得 URL 與對應作者資訊
        print("3/5: 正在解析檔案內容以取得 URL、作者、摘要、期刊與發表年份資訊...")
        url_author_list = []
        current_author = None
        current_abstract = None
        current_journal = None
        current_year = None
        lines = content.splitlines()
        for line in lines:
            line = line.strip()
            if line.startswith("論文標題:"):
                current_Title = line.replace("論文標題:", "").strip()
            if line.startswith("作者:"):
                current_author = line.replace("作者:", "").strip()
            elif line.startswith("摘要:"):
                current_abstract = line.replace("摘要:", "").strip()
            elif line.startswith("期刊:"):
                current_journal = line.replace("期刊:", "").strip()
            elif line.startswith("發表年份:"):
                current_year = line.replace("發表年份:", "").strip()
            else:
                match = re.search(r'https?://[^\s]+', line)
            if match:
                url = match.group()
                url_author_list.append((url, current_author, current_abstract, current_journal, current_year))
        
        if not url_author_list:
            print("未找到任何 URL 鏈結")
            logging.info("未找到任何 URL 鏈結")
            return
        else:
            print(f"4/5: 已提取 {len(url_author_list)} 個 URL，正在處理網頁資訊...")
        
        output_file_path = file_path.replace(".txt", "_Detail.txt")
        print(f"4/5: 準備寫入結果到 {output_file_path}...")
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36"
        }
        with open(output_file_path, 'w', encoding='utf-8') as output_file:
            total_urls = len(url_author_list)
            for index, (url, file_author, current_abstract, current_journal, current_year) in enumerate(url_author_list, start=1):
                print(f"\n4/5: 正在處理第 {index}/{total_urls} 個 URL: {url}")
                try:
                    # 發出 HTTP GET 請求取得網頁內容
                    resp = requests.get(url, headers=headers, timeout=10)
                    if resp.status_code != 200:
                        output_file.write(f"URL: {url}\n")
                        output_file.write(f"Title: {current_Title}\n")
                        output_file.write(f"Author: {current_author}\n")
                        output_file.write(f"Abstract: {current_abstract}\n")
                        output_file.write(f"Journal: {current_journal}\n")
                        output_file.write(f"Publish_Year: {current_year}\n")
                        raise Exception(f"HTTP 狀態碼: {resp.status_code}")
                    
                    html = resp.text
                    # 解析 HTML
                    try:
                        soup = BeautifulSoup(html, 'html.parser')
                        
                        # 取得網頁標題，若無標題則使用預設值
                        title_tag = soup.find('title')
                        title = title_tag.get_text(strip=True) if title_tag else "無標題"
                        print(f"  成功載入 {url}，標題：{title}")
                        
                        # 取得 meta author（若有）
                        meta_author = soup.find("meta", attrs={"name": "author"})
                        author_from_site = meta_author["content"].strip() if meta_author and meta_author.get("content") else None
                        author = author_from_site if author_from_site else (file_author if file_author else "Unknown Author")
                        
                        # 取得 meta description 作為摘要，若無則回傳預設值
                        meta_desc = soup.find("meta", attrs={"name": "description"})
                        abstract = meta_desc["content"].strip() if meta_desc and meta_desc.get("content") else current_abstract
                        
                        output_file.write(f"URL: {url}\n")
                        output_file.write(f"Title: {title}\n")
                        output_file.write(f"Author: {author}\n")
                        output_file.write(f"Abstract: {abstract}\n")
                        output_file.write(f"Journal: {current_journal}\n")
                        output_file.write(f"Publish_Year: {current_year}\n")
                        output_file.write("=" * 80 + "\n")                    
                        
                        # 如有需要，可休息一段時間以減緩請求頻率
                        time.sleep(1)
                    except Exception as e:
                        # 若處理網頁時發生錯誤，將原本從 txt 抓取的資料寫入
                        output_file.write(f"URL: {url}\n")
                        output_file.write(f"Title: 無法取得標題\n")
                        output_file.write(f"Author: {file_author if file_author else 'Unknown Author'}\n")
                        output_file.write(f"Abstract: {current_abstract if current_abstract else '無摘要'}\n")
                        output_file.write(f"Journal: {current_journal if current_journal else '無期刊'}\n")
                        output_file.write(f"Publish_Year: {current_year if current_year else '無年份'}\n")
                        output_file.write(f"Error: {str(e)}\n")
                        output_file.write("=" * 80 + "\n")
                        logging.error("處理 %s 時發生錯誤: %s", url, str(e))
                        print(f"  處理 {url} 時發生錯誤: {str(e)}")
                except Exception as e:
                    output_file.write(f"URL: {url}\n")
                    output_file.write(f"Error: {str(e)}\n")
                    output_file.write("=" * 80 + "\n")
                    logging.error("處理 %s 時發生錯誤: %s", url, str(e))
                    print(f"  處理 {url} 時發生錯誤: {str(e)}")
        
        print(f"\n5/5: 處理完成！結果已寫入 {output_file_path}")
        logging.info("處理完成，結果已寫入 %s", output_file_path)
    
    except Exception as e:
        print(f"處理檔案時發生錯誤：{str(e)}")
        logging.error("處理檔案時發生錯誤：%s", str(e))

if __name__ == "__main__":
    extract_urls_from_file()
