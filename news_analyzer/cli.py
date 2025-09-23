import argparse
from typing import Optional
from pathlib import Path
from .core import (
    TavilyClient,
    OpenRouterClient,
    ContentExtractor,
    ReportGenerator,
    FileManager,
    NewsAnalyzerError
)

def main():
    """命令行入口点"""
    parser = argparse.ArgumentParser(description="新闻分析报告生成器")
    parser.add_argument("query", help="搜索关键词")
    parser.add_argument("-o", "--output", help="输出目录", default="reports")
    parser.add_argument("-m", "--max", type=int, help="最大文章数", default=50)
    args = parser.parse_args()

    try:
        # 初始化组件
        tavily = TavilyClient()
        openrouter = OpenRouterClient()
        extractor = ContentExtractor()
        generator = ReportGenerator()
        file_manager = FileManager(args.output)

        # 执行新闻搜索
        print(f"🔍 正在搜索: {args.query}")
        articles = tavily.search_news(args.query, args.max)
        
        # 提取文章内容
        print("📥 正在提取文章内容...")
        for article in articles:
            content, date = extractor.extract_content(article.url)
            article.content = content
            if date:
                article.date = date

        # 生成报告内容
        print("📊 正在生成分析报告...")
        prompt = generator.build_prompt(articles)
        
        # 尝试不同模型直到成功
        html_content = ""
        for model in generator.models:
            try:
                print(f"🧠 尝试使用模型: {model}")
                html_content = openrouter.generate_html(prompt, model)
                if "<html>" in html_content.lower():
                    break
            except Exception as e:
                print(f"⚠️ 模型 {model} 失败: {str(e)}")
                continue

        if not html_content:
            raise NewsAnalyzerError("所有模型均无法生成有效内容")

        # 保存报告
        print("💾 正在保存文件...")
        saved_path = file_manager.save_html(html_content)
        print(f"✅ 报告已保存至: {saved_path}")

    except NewsAnalyzerError as e:
        print(f"❌ 发生错误: {str(e)}")
        exit(1)

if __name__ == "__main__":
    main()
