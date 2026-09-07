# be-sdk-go 不是 brickKit 组件，但仍按总纲 §I 的 9 个门禁目标写——
# 一致性帮 AI：新开会话看到一份陌生 Makefile，不用先猜它和组件仓库的
# Makefile 是不是同一套规矩。
.DEFAULT_GOAL := help
.PHONY: help check-version test image migrate-idempotent dag-check contract-check \
        import-scan smoke module-check all

help:  ## 列出所有目标
	@awk 'BEGIN{FS=":.*##"; printf "\n用法: make <目标>\n\n"} \
	     /^[a-zA-Z0-9_-]+:.*##/ {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2} \
	     /^##@/ {printf "\n\033[1m%s\033[0m\n", substr($$0,5)}' $(MAKEFILE_LIST)
	@echo ""

##@ 9 个门禁里对本仓库没意义的（非组件仓库，没有对应的东西）
check-version:  ## N/A：没有 component.yaml
	@echo "N/A：非组件仓库，没有 component.yaml"

image:  ## N/A：纯横切库，没有可执行文件，不产出部署镜像
	@echo "N/A：纯横切库，没有可执行文件，不产出部署镜像"

migrate-idempotent:  ## N/A：没有迁移
	@echo "N/A：非组件仓库，没有迁移"

contract-check:  ## N/A：没有 contracts/
	@echo "N/A：非组件仓库，没有 contracts/"

smoke:  ## N/A：没有 brickkit up 的对象
	@echo "N/A：非组件仓库，没有 brickkit up 的对象"

module-check:  ## N/A：没有 module.New 契约
	@echo "N/A：非组件仓库，没有 module.New 契约"

##@ 对本仓库真正有意义的
test:  ## 跑全部单测（-race），需要 TEST_PG_DSN（+可选 TEST_NATS_URL）
	go test ./... -race

dag-check:  ## 包依赖图无环（Go 编译器本身就不允许循环 import，这条恒过）
	@go list ./... >/dev/null && echo "✓ 包依赖图无环（Go 编译器本身就不允许循环 import）"

# ⚠️ be-sdk-go 是铁律六 import 扫描的白名单本体（§13.3 铁律六）——它被所有
# 组件依赖，但它自己不许依赖任何组件仓库，否则白名单就变成了传染通道。
import-scan:  ## 铁律六：be-sdk-go 自己不许依赖任何组件仓库
	@bad="$$(go list -deps ./... 2>/dev/null | grep '^github.com/brickKit/' | grep -vE '^github.com/brickKit/be-sdk-go($$|/)')"; \
	if [ -n "$$bad" ]; then \
		echo "✗ be-sdk-go 不许依赖任何组件仓库：$$bad"; exit 1; \
	fi; \
	echo "✓ 零组件依赖"

##@ 汇总
all: check-version test image migrate-idempotent dag-check contract-check import-scan smoke module-check  ## 跑完整 9 项（含上面几条 N/A 直接过）
