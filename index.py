import os
import json
import logging
import random
from datetime import datetime, timezone
from telegram import (
    Update,
    ReplyKeyboardMarkup,
)
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes
)
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
import gspread
import requests
import asyncio
from concurrent.futures import ThreadPoolExecutor

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Environment Variables
TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN')
GOOGLE_SHEET_ID = os.environ.get('GOOGLE_SHEET_ID')
GOOGLE_DRIVE_FOLDER_ID = os.environ.get('GOOGLE_DRIVE_FOLDER_ID')
DEEPSEEK_API_KEY = os.environ.get('DEEPSEEK_API_KEY')

# Create a ThreadPoolExecutor for running synchronous code
executor = ThreadPoolExecutor(max_workers=3)

# Initialize bot globally
bot = Application.builder().token(TOKEN).build()

# State constants
AUDIENCE = 'AUDIENCE'
LANGUAGE = 'LANGUAGE'
COUNTRY = 'COUNTRY'
TOPIC = 'TOPIC'
FORMAT = 'FORMAT'

# Store user states
user_states = {}

def init_google_services():
    """Initialize Google Sheets and Drive services"""
    try:
        credentials_json = json.loads(os.getenv('GOOGLE_CREDENTIALS'))
        scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive'
        ]
        credentials = ServiceAccountCredentials.from_json_keyfile_dict(credentials_json, scope)
        gc = gspread.authorize(credentials)
        sheet = gc.open_by_key(GOOGLE_SHEET_ID).sheet1
        
        drive_service = build('drive', 'v3', credentials=credentials)
        return sheet, drive_service
    except Exception as e:
        logger.error(f"Error initializing Google services: {e}")
        raise

def get_unique_values(sheet, column_name):
    """Get unique values from a specific column"""
    try:
        records = sheet.get_all_records()
        return sorted(list(set(record[column_name] for record in records if record[column_name])))
    except Exception as e:
        logger.error(f"Error getting unique values for {column_name}: {e}")
        return []

def generate_push_notifications(username, product, country, language, app_link, message):
    """Generate push notifications using DeepSeek"""
    try:
        current_time = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        
        prompt = f"""Current Date and Time (UTC - YYYY-MM-DD HH:MM:SS formatted): {current_time}
Current User's Login: {username}

Generate 10 push notification versions for:
Product: {product}
Country: {country}
Language: {language}
App Link: {app_link}
Message: {message}

Requirements:
- Title: max 22 characters
- Body: max 108 characters
- Include country-specific dialect
- Use respectful form of address
- Include call to action
- Use appropriate emojis
- Each version must be unique in meaning
- Provide character count for each
- Include English translation

Format for each version:
[{language}] title text
(character_count) || _English translation_
[{language}] body text
(character_count) || _English translation_"""

        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {DEEPSEEK_API_KEY}"
        }
        
        data = {
            "model": "deepseek-chat",
            "messages": [
                {
                    "role": "system", 
                    "content": "You are Push Generator GPT, a specialized marketing copywriter for push notifications."
                },
                {"role": "user", "content": prompt}
            ],
            "temperature": 0.7,
            "max_tokens": 2000
        }

        with requests.Session() as session:
            response = session.post(
                "https://api.deepseek.com/v1/chat/completions",
                headers=headers,
                json=data,
                timeout=30
            )
            response.raise_for_status()
            return response.json()['choices'][0]['message']['content']

    except requests.RequestException as e:
        logger.error(f"Network error in generate_push_notifications: {e}")
        return None
    except Exception as e:
        logger.error(f"Error generating push content with DeepSeek: {e}")
        return None

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Send a message when the command /start is issued."""
    try:
        keyboard = ReplyKeyboardMarkup(
            [
                ['Generate Content'],
                ['Use push-generator']
            ],
            resize_keyboard=True,
            one_time_keyboard=True
        )
        
        await update.message.reply_text(
            'Welcome! Choose an option:',
            reply_markup=keyboard
        )
        user_states[update.effective_user.id] = {'state': None, 'data': {}}
    except Exception as e:
        logger.error(f"Error in start_command: {e}")
        await update.message.reply_text("An error occurred. Please try again.")

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Send a message when the command /help is issued."""
    help_text = """
Available commands:
/start - Start the bot
/help - Show this help message

Options:
1. Generate Content - Create content based on templates
2. Use push-generator - Generate push notifications
    """
    await update.message.reply_text(help_text)

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle user messages"""
    try:
        user_id = update.effective_user.id
        if user_id not in user_states:
            await update.message.reply_text(
                "Please start over with /start\n"
                "If you need help, use /help command."
            )
            return

        message_text = update.message.text
        state = user_states[user_id].get('state')
        user_data = user_states[user_id].get('data', {})

        if state is None:
            if message_text == 'Generate Content':
                sheet, _ = init_google_services()
                audiences = get_unique_values(sheet, 'audience')
                keyboard = ReplyKeyboardMarkup(
                    [[audience] for audience in audiences],
                    resize_keyboard=True,
                    one_time_keyboard=True
                )
                await update.message.reply_text(
                    "Please select your audience:",
                    reply_markup=keyboard
                )
                user_states[user_id]['state'] = AUDIENCE
                
            elif message_text == 'Use push-generator':
                await update.message.reply_text("What's your product name?")
                user_states[user_id]['state'] = 'awaiting_product'

        elif state == AUDIENCE:
            user_states[user_id]['data']['audience'] = message_text
            sheet, _ = init_google_services()
            languages = get_unique_values(sheet, 'language')
            keyboard = ReplyKeyboardMarkup(
                [[lang] for lang in languages],
                resize_keyboard=True,
                one_time_keyboard=True
            )
            await update.message.reply_text(
                "Great! Now select the language:",
                reply_markup=keyboard
            )
            user_states[user_id]['state'] = LANGUAGE

        elif state == LANGUAGE:
            user_states[user_id]['data']['language'] = message_text
            sheet, _ = init_google_services()
            countries = get_unique_values(sheet, 'country')
            keyboard = ReplyKeyboardMarkup(
                [[country] for country in countries],
                resize_keyboard=True,
                one_time_keyboard=True
            )
            await update.message.reply_text(
                "Perfect! Choose the country:",
                reply_markup=keyboard
            )
            user_states[user_id]['state'] = COUNTRY

        elif state == COUNTRY:
            user_states[user_id]['data']['country'] = message_text
            sheet, _ = init_google_services()
            topics = get_unique_values(sheet, 'topic')
            keyboard = ReplyKeyboardMarkup(
                [[topic] for topic in topics],
                resize_keyboard=True,
                one_time_keyboard=True
            )
            await update.message.reply_text(
                "Choose the topic:",
                reply_markup=keyboard
            )
            user_states[user_id]['state'] = TOPIC

        elif state == TOPIC:
            user_states[user_id]['data']['topic'] = message_text
            sheet, _ = init_google_services()
            formats = get_unique_values(sheet, 'format')
            keyboard = ReplyKeyboardMarkup(
                [[format_] for format_ in formats],
                resize_keyboard=True,
                one_time_keyboard=True
            )
            await update.message.reply_text(
                "Finally, choose the format:",
                reply_markup=keyboard
            )
            user_states[user_id]['state'] = FORMAT

        elif state == FORMAT:
            try:
                sheet, drive_service = init_google_services()
                user_data = user_states[user_id]['data']
                user_data['format'] = message_text

                all_records = sheet.get_all_records()
                matching_records = [
                    record for record in all_records
                    if record['audience'] == user_data['audience']
                    and record['language'] == user_data['language']
                    and record['country'] == user_data['country']
                    and record['topic'] == user_data['topic']
                    and record['format'] == user_data['format']
                ]

                if matching_records:
                    random_record = random.choice(matching_records)
                    response = f"Here's your content:\n\n{random_record['text']}"
                    
                    if 'image_id' in random_record and random_record['image_id']:
                        try:
                            file = drive_service.files().get(
                                fileId=random_record['image_id'],
                                fields='webViewLink'
                            ).execute()
                            response += f"\n\nImage: {file['webViewLink']}"
                        except Exception as e:
                            logger.error(f"Error getting image: {e}")
                    
                    if len(response) > 4096:
                        parts = [response[i:i + 4096] for i in range(0, len(response), 4096)]
                        for part in parts:
                            await update.message.reply_text(part)
                    else:
                        await update.message.reply_text(response)
                else:
                    await update.message.reply_text(
                        "Sorry, no content found matching your criteria. Try different options."
                    )
            except Exception as e:
                logger.error(f"Error in FORMAT state: {e}")
                await update.message.reply_text(
                    "An error occurred while getting content.\n"
                    "Please try again with /start"
                )
            finally:
                del user_states[user_id]

        elif state == 'awaiting_product':
            user_data['product'] = message_text
            await update.message.reply_text("Which country are you targeting?")
            user_states[user_id]['state'] = 'awaiting_country'
            
        elif state == 'awaiting_country':
            user_data['country'] = message_text
            await update.message.reply_text("What language should the copy be in?")
            user_states[user_id]['state'] = 'awaiting_language'
            
        elif state == 'awaiting_language':
            user_data['language'] = message_text
            await update.message.reply_text("Please provide the App Store/Google Play link:")
            user_states[user_id]['state'] = 'awaiting_link'
            
        elif state == 'awaiting_link':
            user_data['app_link'] = message_text
            await update.message.reply_text("What message do you want to convey?")
            user_states[user_id]['state'] = 'awaiting_message'
            
        elif state == 'awaiting_message':
            try:
                user_data['message'] = message_text
                await update.message.reply_text("🔄 Generating push notifications... Please wait.")

                # Use the executor for the synchronous function
                push_content = await asyncio.get_event_loop().run_in_executor(
                    executor,
                    generate_push_notifications,
                    update.effective_user.username or 'anonymous',
                    user_data['product'],
                    user_data['country'],
                    user_data['language'],
                    user_data['app_link'],
                    user_data['message']
                )
                
                if push_content:
                    if len(push_content) > 4096:
                        parts = [push_content[i:i + 4096] for i in range(0, len(push_content), 4096)]
                        for part in parts:
                            await update.message.reply_text(part)
                    else:
                        await update.message.reply_text(push_content)
                    
                    await update.message.reply_text(
                        "Want to generate more push notifications? Type /start"
                    )
                else:
                    await update.message.reply_text(
                        "😕 Sorry, couldn't generate push notifications.\n"
                        "Please try again with /start"
                    )
            except Exception as e:
                logger.error(f"Error in awaiting_message state: {e}")
                await update.message.reply_text(
                    "An error occurred while generating notifications.\n"
                    "Please try again with /start"
                )
            finally:
                del user_states[user_id]

    except Exception as e:
        logger.error(f"Error in handle_message: {e}")
        await update.message.reply_text("An error occurred. Please try again with /start")

async def error(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Log Errors caused by Updates."""
    logger.error(f'Update "{update}" caused error "{context.error}"')
    if update and update.effective_message:
        await update.effective_message.reply_text(
            "Sorry, an error occurred. Please try again with /start"
        )

async def process_telegram_update(update_dict):
    """Process Telegram update"""
    try:
        update = Update.de_json(update_dict, bot.bot)
        await bot.initialize()
        await bot.process_update(update)
    except Exception as e:
        logger.error(f"Error in process_telegram_update: {e}")
        raise
    finally:
        try:
            await bot.shutdown()
        except Exception as e:
            logger.error(f"Error during bot shutdown: {e}")

def handler(event, context):
    """Yandex Cloud Function entry point"""
    if 'body' not in event:
        return {
            'statusCode': 400,
            'body': json.dumps({'error': 'No body found in request'})
        }
    
    try:
        update_dict = json.loads(event['body'])
        
        # Create and set new event loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        # Run the coroutine
        try:
            loop.run_until_complete(process_telegram_update(update_dict))
        except Exception as e:
            logger.error(f"Error processing update: {e}")
            raise
        
        return {
            'statusCode': 200,
            'body': json.dumps({'status': 'ok'})
        }
    except Exception as e:
        logger.error(f'Error in handler: {str(e)}')
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
    finally:
        try:
            # Close the loop
            if loop.is_running():
                loop.stop()
            pending = asyncio.all_tasks(loop)
            if pending:
                loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            if not loop.is_closed():
                loop.close()
        except Exception as e:
            logger.error(f"Error during loop cleanup: {e}")

# Register handlers
bot.add_handler(CommandHandler("start", start_command))
bot.add_handler(CommandHandler("help", help_command))
bot.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
bot.add_error_handler(error)
