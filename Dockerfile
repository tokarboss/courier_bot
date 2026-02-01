# Используем легковесный образ Python
FROM python:3.11-slim

# Устанавливаем системное время (критично для Google API)
RUN apt-get update && apt-get install -y tzdata && \
    ln -fs /usr/share/zoneinfo/Europe/Moscow /etc/localtime && \
    dpkg-reconfigure --frontend noninteractive tzdata

# Устанавливаем рабочую папку
WORKDIR /app

# Копируем файл зависимостей
COPY requirements.txt .

# Устанавливаем библиотеки
RUN pip install --no-cache-dir -r requirements.txt

# Копируем все файлы проекта (включая main.py и credentials.json)
COPY . .

# Команда запуска
CMD ["python", "main.py"]