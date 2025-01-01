from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

# Slack credentials
SLACK_BOT_TOKEN = "xoxb-xxxxxxxxxxxxx-xxxxxxxxxxxxx"
SLACK_CHANNEL = "#testan"  

# Initialize the Slack client
slack_client = WebClient(token=SLACK_BOT_TOKEN)

def send_slack_alert(subject, content):
    """
    Send an alert to the configured Slack channel.

    :param subject: The title of the Slack message
    :param content: The body of the Slack message
    """
    try:
        response = slack_client.chat_postMessage(
            channel=SLACK_CHANNEL,
            text=f"*{subject}*\n{content}"
        )
        print(f"Slack message sent successfully: {response['ts']}")
    except SlackApiError as e:
        print(f"Error sending Slack message: {e.response['error']}")
