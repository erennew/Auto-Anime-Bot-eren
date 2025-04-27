from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardButton, InlineKeyboardMarkup, CallbackQuery
from bot_tasks import handle_task  # Import your task handler for downloading, encoding, etc.
from bot import FILE_STORE  # Assuming FILE_STORE is defined in your config.py

# In-memory storage for user data
user_data = {}

# Initialize the bot (integrating into your existing bot, so no separate bot initialization)
@bot.on_message(filters.command("addlink"))
async def add_link_handler(_, message: Message):
    if len(message.command) < 2:
        await message.reply("ℹ️ Please provide a valid magnet or RSS link.")
        return

    link = message.command[1]

    # Check if it's a valid magnet link
    if link.startswith("magnet:"):
        user_data[message.chat.id] = {"link": link}
        
        # Inline buttons for quality selection
        buttons = [
            [InlineKeyboardButton("480p", callback_data="quality_480p")],
            [InlineKeyboardButton("720p", callback_data="quality_720p")],
            [InlineKeyboardButton("1080p", callback_data="quality_1080p")]
        ]
        
        await message.reply("Please select the video quality:", reply_markup=InlineKeyboardMarkup(buttons))

    elif "rss" in link:
        await message.reply("RSS feeds are not supported yet. Please provide a valid magnet link.")
    else:
        await message.reply("ℹ️ Invalid link format. Please provide a valid magnet link.")

# Inline button for quality selection callback handler
@bot.on_callback_query()
async def handle_button_click(_, callback_query: CallbackQuery):
    user_id = callback_query.from_user.id
    quality = callback_query.data.split("_")[1]  # Get selected quality
    
    # Check if the user data exists (link they provided earlier)
    if user_id in user_data and "link" in user_data[user_id]:
        magnet_link = user_data[user_id]["link"]
        
        # Save quality selection in user data
        user_data[user_id]["quality"] = quality
        
        # Ask if the user wants to rename the file or upload a custom thumbnail
        buttons = [
            [InlineKeyboardButton("Rename File", callback_data="rename_file")],
            [InlineKeyboardButton("Upload Custom Thumbnail", callback_data="upload_thumbnail")]
        ]
        
        await callback_query.message.edit_text(f"Quality selected: {quality}. Would you like to customize the file?",
                                              reply_markup=InlineKeyboardMarkup(buttons))
    else:
        await callback_query.answer("No link found. Please use /addlink to add a magnet link.")

# Handle renaming the file
@bot.on_callback_query(filters.regex('^rename_file$'))
async def handle_rename_file(_, callback_query: CallbackQuery):
    user_id = callback_query.from_user.id
    
    if user_id in user_data and "link" in user_data[user_id]:
        await callback_query.message.edit_text("Please send the new name for the file.")
        
        # Save the action in user data to track the state
        user_data[user_id]["action"] = "rename"
    else:
        await callback_query.answer("No link found. Please use /addlink to add a magnet link.")

# Handle custom thumbnail upload (automatically triggers task)
@bot.on_message(filters.photo)
async def handle_thumbnail(_, message: Message):
    user_id = message.chat.id
    
    if user_id in user_data and "link" in user_data[user_id]:
        # Save the thumbnail (image file ID)
        user_data[user_id]["thumbnail"] = message.photo.file_id
        await message.reply("Thumbnail set successfully!")

        # Proceed with the task (download, encode, upload)
        magnet_link = user_data[user_id]["link"]
        quality = user_data[user_id]["quality"]
        
        # If no custom name provided, keep original
        new_name = user_data[user_id].get("new_name", None)
        thumbnail = user_data[user_id]["thumbnail"]

        # Start the download, encoding, and upload task
        await handle_task(user_id, magnet_link, quality, new_name, thumbnail)
        
        # Notify the user that the task has started
        await message.reply("Task started: Downloading, encoding, and uploading...")

# Handle text responses for renaming the file
@bot.on_message(filters.text)
async def handle_text_response(_, message: Message):
    user_id = message.chat.id
    
    if user_id in user_data and "action" in user_data[user_id]:
        action = user_data[user_id]["action"]
        
        if action == "rename":
            # Rename the file (save the new name)
            user_data[user_id]["new_name"] = message.text
            await message.reply(f"File will be renamed to {message.text}.")
            user_data[user_id]["action"] = None  # Reset action
        else:
            await message.reply("Unknown action. Please use the menu to select actions.")

