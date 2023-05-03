from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import and_, or_, desc
import pandas as pd
import boto3
from io import BytesIO
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders
from fastapi import FastAPI
import threading
from typing import Optional, Tuple
from datetime import datetime
from pydantic import BaseModel
from your_module import ActivityAccounting  # Импорт модели таблицы

app = FastAPI()

DATABASE_URL = "your_database_url"  # Заменить на URL вашей базы данных

engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)

# 1. Запрос с использованием SQLAlchemy.


def get_data(session, date_range=None, user_email=None, admin=None, status=None, transaction_type=None, original_id=None):
    filters = []
    if date_range:
        filters.append(ActivityAccounting.date.between(date_range[0], date_range[1]))
    if user_email:
        filters.append(ActivityAccounting.user_email == user_email)
    if admin:
        filters.append(ActivityAccounting.admin == admin)
    if status:
        filters.append(ActivityAccounting.status == status)
    if transaction_type:
        filters.append(ActivityAccounting.type == transaction_type)
    if original_id:
        filters.append(ActivityAccounting.original_id == original_id)

    query = session.query(ActivityAccounting).filter(and_(*filters)).order_by(desc(ActivityAccounting.date))
    return query.all()


# 2. Функция построения отчета в Excel, сохранение его в S3 и пересылка по smtp.

def create_excel_report(data, email, s3_bucket):
    df = pd.DataFrame([row.__dict__ for row in data])
    output = BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')
    df.to_excel(writer, sheet_name='Sheet1', index=False)
    writer.save()
    output.seek(0)

    s3 = boto3.client('s3')
    key = f'reports/{email}/report.xlsx'
    s3.upload_fileobj(output, s3_bucket, key)

    link = f'https://{s3_bucket}.s3.amazonaws.com/{key}'  # Возможно понадобится добавить регион

    send_email(email, link)


def send_email(email, link):
    from_email = "your_email@example.com"
    to_email = email
    subject = "Your Report is Ready"
    body = f"Your report is ready for download. Click the link below to download it: {link}"

    msg = MIMEMultipart()
    msg['From'] = from_email
    msg['To'] = to_email
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))

    server = smtplib.SMTP('smtp.example.com', 587)
    # Определяем, поддерживает ли сервер TLS
    server.ehlo()
    # Защищаем соединение с помощью шифрования tls
    server.starttls()
    # Повторно идентифицируем себя как зашифрованное соединение перед аутентификацией.
    server.ehlo()
    server.login(from_email, "your_password")
    text = msg.as_string()
    server.sendmail(from_email, to_email, text)
    server.quit()


# 3. Создание ендпойнта и асинхронного вызова функции в дополнительном потоке.

class ReportRequest(BaseModel):
    email: str
    date_range: Optional[Tuple[datetime, datetime]] = None
    user_email: Optional[str] = None
    admin: Optional[str] = None
    status: Optional[str] = None
    transaction_type: Optional[str] = None
    original_id: Optional[str] = None


@app.post("/generate_report")
async def generate_report(report_request: ReportRequest):
    def process_request():
        session = Session()
        data = get_data(session,
                        date_range=report_request.date_range,
                        user_email=report_request.user_email,
                        admin=report_request.admin,
                        status=report_request.status,
                        transaction_type=report_request.transaction_type,
                        original_id=report_request.original_id)
        create_excel_report(data, report_request.email, "your_s3_bucket_name")
        session.close()

    threading.Thread(target=process_request).start()
    return {
        "message": "The report generation has been started. You will receive an email with a download link when the report is ready."
    }
