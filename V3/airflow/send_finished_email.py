import os
import ssl
import smtplib
from dotenv import load_dotenv
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Load environement variables
load_dotenv()

LOCAL_DIR = '/tmp/'


def main():
    # Send email with result
    port = 465  # For SSL
    password = os.environ.get("mdp_mail")
    context = ssl.create_default_context()
    sender_email = "lci.automated.report@gmail.com"
    receiver_email = "Benjamin.Marquis@lcieducation.com"

    message = MIMEMultipart("alternative")
    message["Subject"] = "multipart test"
    message["From"] = sender_email
    message["To"] = receiver_email

    html = f"""
    <html>
        <body>
            <h1> Job Done </h1>
        </body>
    </html>
    """
    part1 = MIMEText(html, "html")

    message.attach(part1)

    with smtplib.SMTP_SSL("smtp.gmail.com", port, context=context) as server:
        server.login(sender_email, password)
        server.sendmail(
            sender_email, receiver_email, message.as_string()
        )

if __name__ == '__main__':
    main()
