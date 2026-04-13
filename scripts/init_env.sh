#!/bin/sh
set -eu

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

if [ -f "$SCRIPT_DIR/.env.example" ]; then
    PROJECT_DIR="$SCRIPT_DIR"
elif [ -f "$SCRIPT_DIR/../.env.example" ]; then
    PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
else
    echo ".env.example 不存在，无法初始化。" >&2
    exit 1
fi

ENV_EXAMPLE="$PROJECT_DIR/.env.example"
ENV_FILE="$PROJECT_DIR/.env"

if [ -f "$ENV_FILE" ]; then
    echo ".env 已存在，跳过初始化。"
    exit 0
fi

# 生成指定长度的随机字符串（a-zA-Z0-9）
generate_secret() {
    length=$1
    LC_ALL=C tr -dc 'A-Za-z0-9' < /dev/urandom | head -c "$length"
}

# 返回需要自动生成的密钥长度，不匹配则返回空
secret_length() {
    case "$1" in
        DJANGO_SECRET_KEY)              echo 64 ;;
        POSTGRES_PASSWORD)              echo 32 ;;
        SIGNER_SHARED_SECRET)           echo 64 ;;
        SIGNER_SECRET_KEY)              echo 64 ;;
        SIGNER_MNEMONIC_ENCRYPTION_KEY) echo 64 ;;
        SIGNER_POSTGRES_PASSWORD)       echo 32 ;;
        *)                              echo "" ;;
    esac
}

# 逐行处理模板
while IFS= read -r line || [ -n "$line" ]; do
    case "$line" in
        *=change-me*)
            key="${line%%=*}"
            len=$(secret_length "$key")
            if [ -n "$len" ]; then
                echo "${key}=$(generate_secret "$len")"
            else
                echo "$line"
            fi
            ;;
        *)
            echo "$line"
            ;;
    esac
done < "$ENV_EXAMPLE" > "$ENV_FILE"

echo "已根据 .env.example 生成 .env，并填充随机密码。"
