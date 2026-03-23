import json
import os
from datetime import datetime, timedelta

class Database:
    def __init__(self, file_path="database.json"):
        self.file_path = file_path
        self.data = self._load()

    def _load(self):
        if os.path.exists(self.file_path):
            with open(self.file_path, "r") as f:
                try:
                    data = json.load(f)
                    # Convert expiry strings back to datetime
                    if "users" in data:
                        for bot_username in data["users"]:
                            for user_id in data["users"][bot_username]:
                                user = data["users"][bot_username][user_id]
                                if "expiry_date" in user and isinstance(user["expiry_date"], str):
                                    user["expiry_date"] = datetime.fromisoformat(user["expiry_date"])
                    return data
                except:
                    return {"users": {}, "bot_settings": {}, "admins": []}
        return {"users": {}, "bot_settings": {}, "admins": []}

    def _save(self):
        # Convert datetime to strings for JSON
        def json_serial(obj):
            if isinstance(obj, (datetime, timedelta)):
                return obj.isoformat()
            raise TypeError ("Type %s not serializable" % type(obj))

        with open(self.file_path, "w") as f:
            json.dump(self.data, f, default=json_serial, indent=4)

    def get_user(self, user_id: int, bot_username: str = "ugdevbot"):
        user_id = str(user_id)
        return self.data["users"].get(bot_username, {}).get(user_id)

    def is_user_authorized(self, user_id: int, bot_username: str = "ugdevbot"):
        from vars import OWNER_ID, ADMINS
        if user_id == OWNER_ID or user_id in ADMINS:
            return True
        user = self.get_user(user_id, bot_username)
        if not user: return False
        expiry = user.get("expiry_date")
        if not expiry: return False
        return expiry > datetime.now()

    def add_user(self, user_id: int, name: str, days: int, bot_username: str = "ugdevbot"):
        user_id = str(user_id)
        if bot_username not in self.data["users"]:
            self.data["users"][bot_username] = {}
        expiry_date = datetime.now() + timedelta(days=days)
        self.data["users"][bot_username][user_id] = {
            "name": name,
            "user_id": int(user_id),
            "expiry_date": expiry_date
        }
        self._save()
        return True, expiry_date

    def remove_user(self, user_id: int, bot_username: str = "ugdevbot"):
        user_id = str(user_id)
        if bot_username in self.data["users"] and user_id in self.data["users"][bot_username]:
            del self.data["users"][bot_username][user_id]
            self._save()
            return True
        return False

    def list_users(self, bot_username: str = "ugdevbot"):
        return list(self.data["users"].get(bot_username, {}).values())

    def is_admin(self, user_id: int):
        from vars import OWNER_ID, ADMINS
        return user_id == OWNER_ID or user_id in ADMINS

    def get_log_channel(self, bot_username: str):
        return self.data["bot_settings"].get(bot_username, {}).get("log_channel")

    def set_log_channel(self, bot_username: str, channel_id: int):
        if bot_username not in self.data["bot_settings"]:
            self.data["bot_settings"][bot_username] = {}
        self.data["bot_settings"][bot_username]["log_channel"] = channel_id
        self._save()
        return True

    def list_bot_usernames(self):
        return list(self.data["users"].keys()) or ["ugdevbot"]

    def is_channel_authorized(self, chat_id: int, bot_username: str):
        return True

    def set_group(self, user_id: int, group_id: int):
        user_id = str(user_id)
        if "user_groups" not in self.data:
            self.data["user_groups"] = {}
        self.data["user_groups"][user_id] = group_id
        self._save()
        return True

    def get_group(self, user_id: int):
        user_id = str(user_id)
        return self.data.get("user_groups", {}).get(user_id)

    def remove_group(self, user_id: int):
        user_id = str(user_id)
        if "user_groups" in self.data and user_id in self.data["user_groups"]:
            del self.data["user_groups"][user_id]
            self._save()
            return True
        return False

    def save_forum_topic(self, group_id: int, topic_title: str, topic_id: int):
        group_id = str(group_id)
        if "forum_topics" not in self.data:
            self.data["forum_topics"] = {}
        if group_id not in self.data["forum_topics"]:
            self.data["forum_topics"][group_id] = {}
        self.data["forum_topics"][group_id][topic_title.strip().lower()] = {
            "topic_id": topic_id,
            "title": topic_title.strip()
        }
        self._save()

    def find_forum_topic(self, group_id: int, topic_title: str):
        group_id = str(group_id)
        entry = self.data.get("forum_topics", {}).get(group_id, {}).get(topic_title.strip().lower())
        if entry:
            return entry["topic_id"]
        return None

    def get_user_expiry_info(self, user_id: int, bot_username: str = "ugdevbot"):
        user = self.get_user(user_id, bot_username)
        if not user: return None
        expiry = user.get('expiry_date')
        days_left = (expiry - datetime.now()).days
        return {
            "name": user.get('name', 'Unknown'),
            "user_id": user_id,
            "expiry_date": expiry.strftime("%d-%m-%Y"),
            "days_left": days_left,
            "is_active": days_left > 0
        }

db = Database()
