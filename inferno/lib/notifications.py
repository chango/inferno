import smtplib
from email.mime.text import MIMEText


def send_mail(job_id=None, job_fail=None, mail_to=None, mail_from=None, mail_server=None,
                retry=None, retry_delay=None):

    mail_from = "Inferno Daemon <inferno@localhost.localdomain>" if not mail_from else mail_from
    if not job_id or not job_fail:
        raise Exception("Empty job failure reason or job id: Cannot continue")
    if not mail_to:
        raise Exception("mail_to cannot be empty: Requires a list of recipient addresses")
    if retry and retry_delay:
        retry_str = ' [AUTO-RETRY IN %s HOUR(s)]' % str(retry_delay)
    mail_server = "localhost" if not mail_server else mail_server
    msg_body = str(job_fail)
    msg = MIMEText(msg_body)
    msg['Subject'] = "Job Status: %s" % job_id + (retry_str if retry_str else '')
    msg['From'] = mail_from
    msg['To'] = ", ".join(mail_to)
    try:
        s = smtplib.SMTP(mail_server)
        s.sendmail(mail_from, mail_to, msg.as_string())
        s.quit()
        return True
    except:
        return False
