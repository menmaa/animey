BINARY_NAME=bootstrap
AWS_LAMBDA_ZIP_NAME=function.zip
AWS_LAMBDA_FUNCTION_NAME=AnimeySourceParser

ifeq ($(OS),Windows_NT)
ZIP_CMD=powershell -NoProfile -Command "Compress-Archive -Path $(BINARY_NAME) -DestinationPath $(AWS_LAMBDA_ZIP_NAME) -Force"
else
ZIP_CMD=zip -j $(AWS_LAMBDA_ZIP_NAME) $(BINARY_NAME)
endif

build:
	GOARCH=amd64 GOOS=linux go build -tags source_parser -o $(BINARY_NAME)
	$(ZIP_CMD)

deploy: build
	aws lambda update-function-code --function-name $(AWS_LAMBDA_FUNCTION_NAME) --zip-file fileb://$(AWS_LAMBDA_ZIP_NAME) --no-cli-pager