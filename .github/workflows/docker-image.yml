name: Docker Image CI

on:
  push:
    branches: [ "main" ]
  pull_request:
    branches: [ "main" ]

jobs:
  build-and-push:
    runs-on: ubuntu-latest
    permissions:
      contents: read
      packages: write  # 必须授予写入 packages (GHCR) 的权限

    steps:
      - name: Checkout repository
        uses: actions/checkout@v4
        
      - name: Download and extract camoufox
        run: |
          # 下载 camoufox
          wget https://github.com/daijro/camoufox/releases/download/v135.0.1-beta.24/camoufox-135.0.1-beta.24-lin.x86_64.zip
          # 解压并重命名为 camoufox-linux
          unzip camoufox-135.0.1-beta.24-lin.x86_64.zip -d camoufox-linux
          
      - name: Log in to GitHub Container Registry
        uses: docker/login-action@v3
        with:
          registry: ghcr.io
          username: ${{ github.actor }}
          password: ${{ secrets.GITHUB_TOKEN }}  # 自动使用 GitHub Token 认证

      - name: Build and push Docker image
        run: |
          # 设置镜像标签
          IMAGE_TAG=0623
          REPO_OWNER=$(echo "${{ github.repository_owner }}" | tr '[:upper:]' '[:lower:]')
          # 构建镜像
          docker build . --file Dockerfile --tag ghcr.io/$REPO_OWNER/build-server:$IMAGE_TAG
          # 推送镜像
          docker push ghcr.io/$REPO_OWNER/build-server:$IMAGE_TAG

