#!/bin/bash

# --- 配置區 (請修改這裡) ---
# ⚠️ 1. 現有的服務名稱 (要從哪裡複製設定)
SERVICE_NAME="prepare-twse-list"
# ⚠️ 2. 區域
REGION="asia-east1"
# ⚠️ 3. 未來要部署的原始碼路徑 (本地資料夾)
SOURCE_CODE_PATH="./"
# ⚠️ 4. 目標服務名稱 (要建立/更新的服務名稱)
TARGET_SERVICE_NAME="prepare-twse-list"
# --- end 配置區 ---

PROJECT_ID=$(gcloud config get-value project)

# 檢查變數是否已修改
if [[ "$SERVICE_NAME" == "YOUR_EXISTING_SERVICE_NAME" ]]; then
    echo "❌ 錯誤：請先編輯腳本，修改 SERVICE_NAME 等配置變數。"
    exit 1
fi

echo "==================================================="
echo "⚙️  正在分析 Cloud Run Service: ${SERVICE_NAME}"
echo "==================================================="

# 1. 獲取 YAML 配置
SERVICE_YAML=$(gcloud run services describe "$SERVICE_NAME" \
    --region "$REGION" \
    --platform managed \
    --format=yaml \
    --project "$PROJECT_ID" 2>/dev/null)

if [ -z "$SERVICE_YAML" ]; then
    echo "❌ 錯誤：找不到服務 ${SERVICE_NAME} 或無權限訪問。"
    exit 1
fi

# 2. 提取參數 (使用 sed 替代 tr 以避免引號錯誤)

# 提取 Service Account
SERVICE_ACCOUNT=$(echo "$SERVICE_YAML" | awk '/serviceAccountName:/ {print $2}' | sed 's/"//g')

# 提取資源限制
CPU_LIMIT=$(echo "$SERVICE_YAML" | awk '/cpu:/ {print $2}' | sed 's/"//g')
MEMORY_LIMIT=$(echo "$SERVICE_YAML" | awk '/memory:/ {print $2}' | sed 's/"//g')
TIMEOUT=$(echo "$SERVICE_YAML" | awk '/timeoutSeconds:/ {print $2}' | sed 's/"//g')

# 提取併發與實例數
CONCURRENCY=$(echo "$SERVICE_YAML" | awk '/containerConcurrency:/ {print $2}' | sed 's/"//g')
MAX_INSTANCES=$(echo "$SERVICE_YAML" | awk '/maxInstanceCount:/ {print $2}' | sed 's/"//g')
MIN_INSTANCES=$(echo "$SERVICE_YAML" | awk '/minInstanceCount:/ {print $2}' | sed 's/"//g')

# 提取 Entrypoint 和 Args
# 使用 sed 去除 [ ] " ' \ 等特殊符號
ENTRYPOINT=$(echo "$SERVICE_YAML" | awk '/command:/ {print $2}' | sed "s/[][\"']//g")
ARGS=$(echo "$SERVICE_YAML" | awk '/args:/ {gsub(/^args: /, ""); print}' | sed "s/[][\"]//g")

# 提取網路設定
VPC_ACCESS_CONNECTOR=$(echo "$SERVICE_YAML" | awk '/connector:/ {print $2}' | sed 's/"//g')
INGRESS=$(echo "$SERVICE_YAML" | awk '/ingress:/ {print $2}' | sed 's/"//g')
AUTH_MODE=$(echo "$SERVICE_YAML" | awk '/run.googleapis.com\/ingress:/ {print $2}' | sed 's/"//g')

# 提取環境變數 (ENV Vars)
# 邏輯：找到 name 和 value，並組合成 KEY=VALUE 格式
ENV_VARS_LIST=$(echo "$SERVICE_YAML" | grep -A 1 "name:" | awk '
    /- name:/ { key=$3 } 
    /- value:/ { val=$0; sub(/.*value: /, "", val); gsub(/"/, "", val); print key"="val }
' | xargs | sed 's/ /,/g')

# 提取 Secrets (僅列出名稱，需手動處理)
SECRETS_LIST_RAW=$(echo "$SERVICE_YAML" | grep "secretName:" | awk '{print $2}' | sed 's/"//g' | sort | uniq | xargs)

# 3. 組合 gcloud 指令

COMMAND="gcloud run deploy ${TARGET_SERVICE_NAME} \\"
COMMAND+=$'\n'"  --region ${REGION} \\"
COMMAND+=$'\n'"  --project ${PROJECT_ID} \\"
COMMAND+=$'\n'"  --source ${SOURCE_CODE_PATH} \\"

# --- 處理可選參數 ---

if [ -n "$SERVICE_ACCOUNT" ] && [ "$SERVICE_ACCOUNT" != "default" ]; then
    COMMAND+=$'\n'"  --service-account ${SERVICE_ACCOUNT} \\"
fi

# 環境變數
if [ -n "$ENV_VARS_LIST" ]; then
    # 注意：這裡使用單引號包覆環境變數列表，防止 Shell 展開錯誤
    COMMAND+=$'\n'"  --set-env-vars \"${ENV_VARS_LIST}\" \\"
fi

# 資源限制
[ -n "$MEMORY_LIMIT" ] && COMMAND+=$'\n'"  --memory ${MEMORY_LIMIT} \\"
[ -n "$CPU_LIMIT" ] && COMMAND+=$'\n'"  --cpu ${CPU_LIMIT} \\"
[ -n "$TIMEOUT" ] && COMMAND+=$'\n'"  --timeout ${TIMEOUT} \\"

# 擴展與併發
[ -n "$CONCURRENCY" ] && COMMAND+=$'\n'"  --concurrency ${CONCURRENCY} \\"
[ -n "$MAX_INSTANCES" ] && COMMAND+=$'\n'"  --max-instances ${MAX_INSTANCES} \\"
[ -n "$MIN_INSTANCES" ] && COMMAND+=$'\n'"  --min-instances ${MIN_INSTANCES} \\"

# 網路與權限
[ -n "$VPC_ACCESS_CONNECTOR" ] && COMMAND+=$'\n'"  --vpc-connector ${VPC_ACCESS_CONNECTOR} \\"

if [ "$INGRESS" == "internal" ]; then
    COMMAND+=$'\n'"  --ingress internal \\"
elif [ "$INGRESS" == "internal-and-cloud-load-balancing" ]; then
    COMMAND+=$'\n'"  --ingress internal-and-cloud-load-balancing \\"
fi

if [[ "$AUTH_MODE" == *"allow-unauthenticated"* ]] || [[ "$INGRESS" == "all" ]]; then
    # 這裡做一個簡單判斷，通常建議明確指定
    COMMAND+=$'\n'"  --allow-unauthenticated \\"
fi

# Entrypoint
if [ -n "$ENTRYPOINT" ]; then
    COMMAND+=$'\n'"  --entrypoint \"${ENTRYPOINT}\" \\"
fi
if [ -n "$ARGS" ]; then
    COMMAND+=$'\n'"  --args \"${ARGS}\" \\"
fi

# 移除最後一個反斜線 (為了美觀)
COMMAND=${COMMAND%\\}

echo ""
echo "✅ 生成的部署指令如下："
echo "---------------------------------------------------"
echo "$COMMAND"
echo "---------------------------------------------------"

if [ -n "$SECRETS_LIST_RAW" ]; then
    echo ""
    echo "⚠️  注意：偵測到此服務使用了 Secrets，您需要手動補上 Secrets設定："
    echo "   涉及的 Secret 名稱: $SECRETS_LIST_RAW"
    echo "   範例參數: --set-secrets /mount/path=SECRET_NAME:latest"
fi