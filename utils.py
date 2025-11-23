import os
import msgpack
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def safe_load_msgpack(file):
    """安全加载 MessagePack 格式的文件，捕获异常并返回空字典"""
    try:
        with open(file, 'rb') as f:
            return msgpack.load(f)
    except (msgpack.exceptions.UnpackException, EOFError) as e:
        logging.error(f"读取文件 {file} 时出错: {e}")
        return {}

def save_hashes_in_batches(hash_list, batch_size=50000):
    """分批保存哈希值到文件"""
    current_size = 0
    while hash_list:
        batch = hash_list[:batch_size]
        hash_list = hash_list[batch_size:]
        
        with open('dist/hash_list.bin', 'ab') as f:
            try:
                msgpack.dump(batch, f)
                current_size += len(batch)
                logging.info(f"✅ 保存了 {len(batch)} 个哈希值, 当前哈希列表大小: {current_size}")
            except Exception as e:
                logging.error(f"⚠ 保存文件时出错: {e}")
                break

def check_and_save_hashes(hash_list):
    """检查哈希列表为空的情况并安全保存"""
    if not hash_list:
        logging.warning("⚠ 哈希列表为空，无法保存数据")
        return

    with open('dist/hash_list.bin', 'wb') as f:
        try:
            msgpack.dump(hash_list, f)
            logging.info(f"✅ 保存了 {len(hash_list)} 个哈希值")
        except Exception as e:
            logging.error(f"⚠ 保存文件时出错: {e}")
