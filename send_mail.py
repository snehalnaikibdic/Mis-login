import logging
# from fastapi import APIRouter, Depends, HTTPException, Response
from decouple import config as dconfig
from fastapi.encoders import jsonable_encoder
from errors import ErrorCodes

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
logger = logging.getLogger(__name__)

# router = APIRouter()

MAIL_USERNAME = str(dconfig('MAIL_USERNAME'))
MAIL_PASSWORD = str(dconfig('MAIL_PASSWORD'))
MAIL_FROM = str(dconfig('MAIL_FROM'))
MAIL_PORT = int(dconfig('MAIL_PORT'))
MAIL_SERVER = str(dconfig('MAIL_SERVER'))
MAIL_FROM_NAME = str(dconfig('MAIL_FROM_NAME'))


SUCCESS_BODY_TEXT = "<html><div style='width:600px;margin-top:30px;margin-left:62px'><p style='color:#333333;font-size: 16px;'><b>Dear {user}</b>,</p>\
                    <p style='font-size: 16px;color:#333333;'> Good day!</p> <p style='font-size: 16px;color:#333333;padding-bottom:10px;'><b>{fileName} </b>is processed for Customer: <b>{customer}</b> by User: <b>{username}</b></p>\
                <p style='font-size: 16px;color:#333333;padding-bottom:10px;'> Your request file <b>{insertFileName}</b> has been processed and response is available to access in the output folder.<br/>\
                               After you download it, the response file will be moved to the delivered folder. You can access it from the delivered folder for future uses. </p>\
                <p style='font-size: 16px;color:#333333;padding-bottom:10px;'>Below is the status of the file.</p>\
                <table style='width: 735px; border:1px solid black;border-collapse: collapse;'> <tbody style='border:1px solid black;border-collapse: collapse;'>\
                    <tr style='border:1px solid black;border-collapse: collapse;'>\
                        <th style='border:1px solid black;border-collapse: collapse;'>Title</th>\
                        <th style='border:1px solid black;border-collapse: collapse;'>Count</th>\
                        <th style='border:1px solid black;border-collapse: collapse;'>Reason for failure cases</th>\
                    </tr>\
                    <tr style='border:1px solid black;border-collapse: collapse;'>\
                        <td style='border:1px solid black;border-collapse: collapse;'>Number of records successfully<br> processed</td>\
                        <td style='border:1px solid black;border-collapse: collapse;'>{recordSuccessCount}</td>\
                        <td style='border:1px solid black;border-collapse: collapse;'></td>\
                    </tr>\
                    <tr style='border:1px solid black;border-collapse: collapse;'>\
                        <td style='border:1px solid black;border-collapse: collapse;'>Number of records failed to be processed</td>\
                        <td style='border:1px solid black;border-collapse: collapse;'>{recordFailedCount}</td>\
                        <td style='border:1px solid black;border-collapse: collapse;'>{listReasonsApplicable}</td>\
                    </tr>\
                    <tr style='border:1px solid black;border-collapse: collapse;'>\
                        <td style='border:1px solid black;border-collapse: collapse;'>Total</td>\
                        <td style='border:1px solid black;border-collapse: collapse;'>{totalCount}</td>\
                        <td style='border:1px solid black;border-collapse: collapse;'></td>\
                    </tr>\
                    </tbody>\
                </table>\
                    <p style='font-size: 16px;color:#333333;padding-bottom:10px;'>Sincerely, <br>\
                    Team IBDIC<br>\
                    support@ibdic.in</p>\
            </div>\
            </font>\
            </html>"

REJECT_BODY_TEXT =  """
                <html>
                <div style='width:600px;margin-top:30px;margin-left:62px'><p style='color:#333333;font-size: 16px;'>
                <p style='color:#333333;font-size: 16px;'>Dear <b>{user}</b>,</p>
                <p style='font-size: 16px;color:#333333;padding-bottom:10px;'> Your request file <b>{insertFileName}</b> has been rejected and response file is log  in the error output folder.<br/>\
                After you correct the file , Kindly re-upload file to input folder. </p>
                <p style='font-size: 16px;color:#333333;padding-bottom:10px;'>Sincerely, <br>\
                    Team IBDIC<br>\
                    support@ibdic.in</p>\
                </div><html>
                """
INVALID_BODY_TEXT =  """
                <html>
                <div style='width:600px;margin-top:30px;margin-left:62px'><p style='color:#333333;font-size: 16px;'>
                <p style='color:#333333;font-size: 16px;'>Dear <b>{user}</b>,</p>
                <p style='font-size: 16px;color:#333333;padding-bottom:10px;'> Your request file <b>{insertFileName}</b> has been rejected and is moved to error input folder folder.<br/>\
                After you correct the file , Kindly re-upload file to input folder. </p>
                <p style='font-size: 16px;color:#333333;padding-bottom:10px;'>Sincerely, <br>\
                    Team IBDIC<br>\
                    support@ibdic.in</p>\
                </div><html>
                """

CORPORATE_GENERATE_OTP_BODY_TEXT = """
                <html>
                <div style='width:600px;margin-top:30px;margin-left:62px'><p style='color:#333333;font-size: 16px;'>
                <p style='color:#333333;font-size: 16px;'>Dear <b>Corporate User</b>,</p>
                <p style='font-size: 16px;color:#333333;padding-bottom:10px;'> Thanks for verifying your <b>{email}</b> account.<br/>\
                    Your Code is:  <b>{otp}</b> </p>
                <p style='font-size: 16px;color:#333333;padding-bottom:10px;'>Sincerely, <br>\
                    Team IBDIC<br>\
                    support@ibdic.in</p>\
                </div><html>
                """

def send_email_async(subject: str, email_to: str, body: dict, body_text=SUCCESS_BODY_TEXT):
    try:
        # Email configuration for Gmail SMTP server
        smtp_server = MAIL_SERVER
        smtp_port = MAIL_PORT
        sender_email = MAIL_FROM
        receiver_email = email_to
        password = MAIL_PASSWORD

        body_data = jsonable_encoder(body)

        # Create message container
        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = receiver_email
        msg['Subject'] = subject

        # Email body
        body = body_text.format(**body_data)

        msg.attach(MIMEText(body, 'html'))
        #import pdb;pdb.set_trace()
        # Start the SMTP session
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(MAIL_USERNAME, MAIL_PASSWORD)
            text = msg.as_string()
            server.sendmail(sender_email, receiver_email, text)
            logger.info('Email sent successfully!')
    except Exception as e:
        logger.exception(f"getting error while sftp file process email send {e}")
        return {**ErrorCodes.get_error_response(500)}
