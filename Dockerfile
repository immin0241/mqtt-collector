FROM golang:1.27-alpine AS builder

# Git, SSL 인증서 등 빌드에 필요한 도구 설치 (필요한 경우)
RUN apk update && apk add --no-cache git ca-certificates tzdata

# 작업 디렉토리 설정
WORKDIR /app

# 의존성 파일 복사 및 다운로드 (고 패키지 캐싱 활용)
COPY go.mod go.sum ./
RUN go mod download

# 소스 코드 복사
COPY . .

# 정적 바이너리로 컴파일 (CGO를 비활성화하여 에러 방지)
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-w -s" -o main .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-w -s" -o api ./cmd/api
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-w -s" -o mcp ./cmd/mcp

# ==========================================
# 2단계: 실행 환경 (Run Stage)
# ==========================================
# 최소 가벼운 이미지인 scratch 또는 alpine 사용 (여기서는 alpine 사용)
FROM alpine:3.19

RUN apk add --no-cache ca-certificates tzdata

WORKDIR /root/

# 빌드 단계에서 생성된 정적 바이너리 파일만 복사
COPY --from=builder /app/main .
COPY --from=builder /app/api .
COPY --from=builder /app/mcp .

# 컨테이너 시작 시 실행될 명령어
CMD ["./main"]