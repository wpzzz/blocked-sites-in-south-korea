import asyncio
import aiohttp
import time
import logging
from aiohttp import TCPConnector

# 并发限制
CONCURRENT_REQUESTS = 50
# 连接池大小
CONNECTION_LIMIT = 100
# 每批次处理的 URL 数量
BATCH_SIZE = 1000

# 设置日志配置
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[
    logging.FileHandler("log.log"),
    logging.StreamHandler()
])

async def fetch_url(session, url, output_file):
    start_time = time.time()
    try:
        async with session.get(url, timeout=5, allow_redirects=False) as response:
            end_time = time.time()
            duration = end_time - start_time
            if response.status == 200:
                text = await response.text()
                if "warning.or.kr/i1.html" in text:
                    logging.info(f"Keyword found in {url} (took {duration:.2f} seconds)")
                    with open(output_file, 'a', encoding='utf-8') as f:
                        f.write(f"{url} (took {duration:.2f} seconds)\n")
            elif response.status in (301, 302, 303, 307, 308):
                location = response.headers.get('Location')
                logging.info(f"Redirect found for {url} to {location} (took {duration:.2f} seconds)")
            else:
                logging.info(f"Non-200 status code {response.status} for {url} (took {duration:.2f} seconds)")
    except Exception as e:
        logging.error(f"Error accessing {url}: {e}")

async def bound_fetch(sem, session, url, output_file):
    async with sem:
        await fetch_url(session, url, output_file)

def clean_url(url):
    if not url.startswith(('http://', 'https://')):
        url = 'http://' + url
    return url

async def process_urls(urls, output_file):
    sem = asyncio.Semaphore(CONCURRENT_REQUESTS)
    connector = TCPConnector(limit=CONNECTION_LIMIT)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = []
        for url in urls:
            cleaned_url = clean_url(url.strip())
            if cleaned_url:
                task = bound_fetch(sem, session, cleaned_url, output_file)
                tasks.append(task)
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            logging.error("Tasks were cancelled due to asyncio.CancelledError")
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

async def process_file_in_batches(input_file, output_file):
    with open(input_file, 'r', encoding='utf-8') as file:
        batch = []
        for line in file:
            batch.append(line.strip())
            if len(batch) >= BATCH_SIZE:
                await process_urls(batch, output_file)
                batch = []
        if batch:
            await process_urls(batch, output_file)

def main():
    input_file = 'domains.txt'
    output_file = 'output.txt'

    # 清空输出文件
    open(output_file, 'w').close()

    # 创建事件循环并运行处理
    loop = asyncio.get_event_loop()
    loop.run_until_complete(process_file_in_batches(input_file, output_file))

if __name__ == '__main__':
    start_time = time.time()
    main()
    logging.info(f"Completed in {time.time() - start_time} seconds")

