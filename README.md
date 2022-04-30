# [WIP] fontanka
Проект по анализу комментариев

## Setup

Для запуска конвееров должен быть установлен ploomber
```sh
pip install ploomber
```

Можно попробовать установить виртуальное окружение
```sh
ploomber install --create-env
# activate environment (unix)
source {path-to-venv}/bin/activate
# activate environment (windows cmd.exe)
{path-to-venv}\Scripts\activate.bat
# activate environment (windows PowerShell)
{path-to-venv}\Scripts\Activate.ps1
```

Ставим пакеты
```
pip install -r requirements.txt
```


## Запуск конвееров

### Для скачивания новостей
```sh
ploomber build -e pipeline_scrape_json.make
```

### Для скачивания комментариев
WIP
