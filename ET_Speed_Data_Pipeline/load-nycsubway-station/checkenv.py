import os
from dotenv import load_dotenv, find_dotenv

# 查找并显示 .env 文件路径
env_path = find_dotenv()
if env_path:
    print("Found .env file at:", env_path)
else:
    print("No .env file found.")

# 尝试加载找到的 .env 文件
if load_dotenv(env_path):
    print("Environment variables loaded successfully.")
else:
    print("Failed to load environment variables.")

# 打印具体的环境变量值
print("Bucket:", os.getenv('DATA_PREPARED_BUCKET'))
