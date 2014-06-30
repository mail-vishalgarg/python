from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from smtplib import SMTP
 
def notify(send_from,send_to, subject, text, files=[]):
  msg = MIMEMultipart()
  msg['Subject'] = subject
  msg['From'] = send_from
  msg['To'] = send_to
  part = MIMEText(text)
  msg.attach(part)

  for f in files:
    part = MIMEApplication(open(f,"rb").read())
    part.add_header('Content-Disposition', 'attachment', filename=f)
    msg.attach(part)

  smtp = SMTP("mailhost.pimco.imswest.sscims.com")
  smtp.sendmail(msg['From'], msg['To'],msg.as_string())
  smtp.quit()
