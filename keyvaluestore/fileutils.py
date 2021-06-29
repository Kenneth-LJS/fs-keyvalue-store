from pathlib import Path
import os
import json

def mkdir_for_path(file_path):
    # file path is a path to a file, not directory
    file_path_obj = Path(file_path)
    file_path_obj.parent.mkdir(parents=True, exist_ok=True)


def folder_exists(dir):
    return os.path.isdir(dir)


def file_exists(dir):
    return os.path.isfile(dir)


def get_folders(dir):
    return [f for f in os.listdir(dir) if folder_exists(os.path.join(dir, f))]


def get_files(dir):
    return [f for f in os.listdir(dir) if file_exists(os.path.join(dir, f))]


def move_file(src, dst):
    os.replace(src, dst)


def delete_file(path):
    os.remove(path)


def write_json_to_file(file_path, data, json_dump_args={}, backup_file_path=None):
    if backup_file_path is None:
        write_json_to_file_direct(file_path, data=data, json_dump_args=json_dump_args)
    else:
        write_json_to_file_with_backup(file_path, backup_file_path=backup_file_path, data=data, json_dump_args=json_dump_args)


def load_json_from_file(file_path, backup_file_path=None, default=None):
    if not os.path.isfile(file_path):
        if not os.path.isfile(backup_file_path):
            return default
        file_path = backup_file_path

    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def write_json_to_file_direct(file_path, data, json_dump_args={}):
    mkdir_for_path(file_path)
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, **json_dump_args)


def write_json_to_file_with_backup(file_path, backup_file_path, data, json_dump_args={}):
    mkdir_for_path(backup_file_path)
    write_json_to_file_direct(backup_file_path, data=data, json_dump_args=json_dump_args)
    
    mkdir_for_path(file_path)
    os.replace(backup_file_path, file_path)