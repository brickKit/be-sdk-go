# be-sdk-go 不是 brickKit 组件，但仍按总纲 §I 的 9 个门禁目标写——
# 一致性帮 AI：新开会话看到一份陌生 Makefile，不用先猜它和组件仓库的
# Makefile 是不是同一套规矩。
.PHONY: check-version test image migrate-idempotent dag-check contract-check \
        import-scan smoke module-check all

check-version:
	@echo "N/A：非组件仓库，没有 component.yaml"

test:
	go test ./... -race

image:
	@echo "N/A：纯横切库，没有可执行文件，不产出部署镜像"

migrate-idempotent:
	@echo "N/A：非组件仓库，没有迁移"

dag-check:
	@go list ./... >/dev/null && echo "✓ 包依赖图无环（Go 编译器本身就不允许循环 import）"

contract-check:
	@echo "N/A：非组件仓库，没有 contracts/"

# ⚠️ be-sdk-go 是铁律六 import 扫描的白名单本体（§13.3 铁律六）——它被所有
# 组件依赖，但它自己不许依赖任何组件仓库，否则白名单就变成了传染通道。
import-scan:
	@bad="$$(go list -deps ./... 2>/dev/null | grep '^github.com/brickKit/' | grep -vE '^github.com/brickKit/be-sdk-go($$|/)')"; \
	if [ -n "$$bad" ]; then \
		echo "✗ be-sdk-go 不许依赖任何组件仓库：$$bad"; exit 1; \
	fi; \
	echo "✓ 零组件依赖"

smoke:
	@echo "N/A：非组件仓库，没有 brickkit up 的对象"

module-check:
	@echo "N/A：非组件仓库，没有 module.New 契约"

all: check-version test image migrate-idempotent dag-check contract-check import-scan smoke module-check
