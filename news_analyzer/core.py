import os
import time
import requests
from bs4 import BeautifulSoup
from typing import List, Dict, Optional
from pathlib import Path
from dotenv import load_dotenv
from pydantic import BaseModel, Field

# 加载环境变量
load_dotenv()

class NewsArticle(BaseModel):
    """新闻文章数据结构"""
    title: str
    url: str
    content: str
    date: str = "不明"
    source: str = "Tavily"

class APIClient:
    """API客户端基类"""
    def __init__(self):
        self.base_url: Optional[str] = None
        self.headers: dict = {}
        self.max_retries: int = 5
        self.retry_delay: int = 2

    def _handle_response(self, response: requests.Response) -> dict:
        """统一处理API响应"""
        if 400 <= response.status_code < 500:
            raise ClientError(f"客户端错误: {response.status_code}")
        if response.status_code >= 500:
            raise ServerError(f"服务器错误: {response.status_code}")
        return response.json()

    def _retry_request(self, method: str, endpoint: str, **kwargs) -> dict:
        """带重试机制的请求处理"""
        for attempt in range(self.max_retries):
            try:
                response = requests.request(
                    method, 
                    f"{self.base_url}{endpoint}",
                    headers=self.headers,
                    **kwargs
                )
                return self._handle_response(response)
            except (requests.exceptions.RequestException, ServerError) as e:
                if attempt == self.max_retries - 1:
                    raise
                delay = self.retry_delay * (2 ** attempt)
                time.sleep(delay)
        raise MaxRetriesError("达到最大重试次数")

class TavilyClient(APIClient):
    """Tavily新闻搜索客户端"""
    def __init__(self):
        super().__init__()
        self.base_url = "https://api.tavily.com"
        self.api_key = os.getenv("TAVILY_API_KEY")

    def search_news(self, query: str, max_results: int = 100) -> List[NewsArticle]:
        """执行新闻搜索"""
        payload = {
            "api_key": self.api_key,
            "query": query,
            "search_depth": "basic",
            "max_results": max_results
        }
        results = self._retry_request("POST", "/search", json=payload)
        return self._parse_results(results.get("results", []))

    def _parse_results(self, results: List[dict]) -> List[NewsArticle]:
        """解析API返回结果"""
        return [
            NewsArticle(
                title=res.get("title", "无标题"),
                url=res["url"],
                content=res.get("content", ""),
                date=res.get("published_date", "不明")
            ) for res in results
        ]

class OpenRouterClient(APIClient):
    """OpenRouter AI客户端"""
    def __init__(self):
        super().__init__()
        self.base_url = "https://openrouter.ai/api/v1"
        self.api_key = os.getenv("OPENROUTER_API_KEY")
        self.headers.update({
            "Authorization": f"Bearer {self.api_key}",
            "HTTP-Referer": os.getenv("APP_URL", "http://localhost"),
            "X-Title": "News Analyzer"
        })

    def generate_html(self, prompt: str, model: str) -> str:
        """生成HTML内容"""
        payload = {
            "model": model,
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 8000
        }
        response = self._retry_request("POST", "/chat/completions", json=payload)
        return self._parse_response(response)

    def _parse_response(self, response: dict) -> str:
        """解析AI响应"""
        if "choices" in response:
            return response["choices"][0]["message"]["content"]
        if "candidates" in response:
            return response["candidates"][0]["content"]["parts"][0]["text"]
        raise InvalidResponseError("无法解析的响应格式")

class ContentExtractor:
    """新闻内容提取器"""
    def __init__(self):
        self.headers = {"User-Agent": "Mozilla/5.0"}

    def extract_content(self, url: str) -> tuple[str, str]:
        """提取文章内容和发布日期"""
        try:
            response = requests.get(url, headers=self.headers, timeout=15)
            response.raise_for_status()
            return self._parse_content(response.text)
        except Exception as e:
            raise ContentExtractionError(f"内容提取失败: {str(e)}")

    def _parse_content(self, html: str) -> tuple[str, str]:
        """解析HTML内容"""
        soup = BeautifulSoup(html, "html.parser")
        date = self._find_publish_date(soup)
        content = "\n".join([
            p.get_text().strip() 
            for p in soup.find_all("p") 
            if len(p.get_text().strip()) > 50
        ])
        return content.strip(), date

    def _find_publish_date(self, soup: BeautifulSoup) -> str:
        """查找发布日期"""
        for meta in ["article:published_time", "pubdate", "publish-date"]:
            tag = soup.find("meta", property=meta) or soup.find("meta", attrs={"name": meta})
            if tag and tag.get("content"):
                return tag["content"]
        return ""

class ReportGenerator:
    """HTML报告生成器"""
    HTML_TEMPLATE = """<!DOCTYPE html>
    <html lang="zh-TW">
    <head>
        <meta charset="UTF-8">
        <title>{title} - 金融分析报告</title>
        <!-- 样式和脚本 -->
    </head>
    <body>
        <!-- 报告内容 -->
    </body>
    </html>"""

    def __init__(self):
        self.models = [
            "google/gemini-2.5-pro-exp-03-25:free",
            "meta-llama/llama-4-maverick:free",
            "deepseek/deepseek-chat-v3-0324:free"
        ]

    def build_prompt(self, articles: List[NewsArticle]) -> str:
        """构建生成提示"""
        articles_content = "\n\n".join(
            f"## {article.title}\n"
            f"日期: {article.date}\n"
            f"来源: {article.url}\n"
            f"内容:\n{article.content}" 
            for article in articles
        )
        return (
            "作为资深金融分析师，请根据以下新闻生成专业HTML报告：\n"
            "1. 使用繁体中文和台湾常用术语\n"
            "2. 包含标题、日期、来源、完整内容和深入分析\n"
            "3. 按日期排序并包含作者信息\n"
            "4. 符合HTML5标准并支持响应式设计\n\n"
            f"{articles_content}"
        )

class FileManager:
    """文件管理类"""
    def __init__(self, output_dir: str = None):
        self.output_dir = Path(output_dir or os.getenv("OUTPUT_DIR", "reports"))
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def save_html(self, content: str, filename: str = "report.html") -> Path:
        """保存HTML文件"""
        file_path = self.output_dir / filename
        try:
            cleaned = self._clean_html(content)
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(cleaned)
            return file_path
        except Exception as e:
            raise FileOperationError(f"文件保存失败: {str(e)}")

    def _clean_html(self, content: str) -> str:
        """清理HTML内容"""
        return content.replace("```html", "").replace("```", "").strip()

# 自定义异常类
class NewsAnalyzerError(Exception): pass
class ClientError(NewsAnalyzerError): pass
class ServerError(NewsAnalyzerError): pass
class MaxRetriesError(NewsAnalyzerError): pass
class InvalidResponseError(NewsAnalyzerError): pass
class ContentExtractionError(NewsAnalyzerError): pass
class FileOperationError(NewsAnalyzerError): pass
