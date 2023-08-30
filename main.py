import os
import sqlite3
import asyncio
import sys
from typing import List, Tuple, Dict, Optional, Union, Any
from random import randint

import telethon
from telethon import TelegramClient, events, errors
from telethon.tl.functions.messages import GetPeerDialogsRequest

# ==========================INFORMATION==========================#

ME: str = 'my_nickname'
MY_ID: int = 0000000

main_api_id_hash: List[Tuple[int, str]] = [] # (api_id, hash_id)...

others_api_id_hash: List[Tuple[int, str]] = [] # (api_id, hash_id)...


sep: str = '\n================================\n'

info_text: str = """**General information:**
     Participants: {}
     Get a new message: {}%
**After:**
     Interested in: {}
     Answered: {}
     Interested in & answered: {}
     Read: {}
     Ignored: {}
**Before:**
     Interested in: {}
     Answered: {}
     Interested in & answered: {}
     Read: {}
     Ignored: {}
**Information about bots:**
     Active bots: {}"""

commands_text: str = """**Commands**:
     __.start__ - show the commands
     __.info__ - show the information
     __.update_info__ - update the information
     __.update_bd__ - update the data bases
     __.test_message__ - test-sending the message to yourself
     __.add_new_users__ - add new users from file
     __.sending_message__ - sending message to the users"""

ans = """По всем вопросам пишите @{}"""


# ==========================MAIN CLIENT CLASS==========================#

class MainClient:

    def __init__(self, name: str, api_id: int, api_hash: str,
                 users: Dict[str, Dict[str, Any]], clients: Dict[str, Dict[str, Any]],
                 workers: List[Tuple[int, str]]) -> None:

        self.client: TelegramClient = TelegramClient(name, api_id, api_hash)
        self.users: Dict[str, Dict[str, Any]] = users
        self.clients: Dict[str, Dict[str, Any]] = clients
        self.workers: List[Tuple[int, str]] = workers
        self.bots: List[Bot] = list()
        self.active_users: List[dict] = list()

        self._name: str = name
        self._api_id: int = api_id
        self._api_hash: str = api_hash
        self._ready_bots_update: int = 0
        self._ready_bots_sending: int = 0

        self.update_users()
        self.create_bots()

    async def get_info_from_bots(self, updated_users: Dict[str, Dict[str, Any]], from_func: str):
        self.users.update(updated_users)
        if from_func == "sending":
            self._ready_bots_sending += 1

            if self._ready_bots_sending == len(self.workers):
                print("message was sent to the all users\ndata base is updating now", end=sep)
                update_messages_id(self.users)
                print("data base was updated", end=sep)
                self._ready_bots_sending = 0

        elif from_func == "updating":
            self._ready_bots_update += 1

            if self._ready_bots_update == len(self.workers):
                print("information was updated\ndata base is updating now", end=sep)
                update_data_base(self.users)
                print("data base was updated", end=sep)
                self._ready_bots_update = 0

    def update_users(self) -> None:
        (self.count_of_win, self.active_users,
         self.count_of_prev_interested, self.count_of_now_interested,
         self.count_of_prev_answered, self.count_of_now_answered,
         self.count_of_prev_read, self.count_of_now_read,
         self.count_of_prev_ans_int, self.count_of_now_ans_int) = get_lists_for_spam(self.users)

    def create_bots(self) -> None:
        api_for_users: Dict[int, Dict[str, Dict[str, Any]]] = dict()
        # {api_id: {username: {column: value, ...}, ...}, ...}

        for user in self.active_users:
            api_for_users.setdefault(user['client_api'], dict())
            api_for_users[user['client_api']].setdefault(user['user_name'], self.users[user['user_name']])

        for i, bot in enumerate(self.workers):
            self.bots.append(Bot(name=r"sessions/bot_account_{}_{}".format(bot[0], i),
                                 api_id=bot[0],
                                 api_hash=bot[1],
                                 bot_users=api_for_users.get(bot[0]),
                                 inherit=self))

    async def run(self):

        @self.client.on(events.NewMessage(outgoing=True, pattern=r'\.start'))
        async def handler(event):
            await event.reply(commands_text)

        @self.client.on(events.NewMessage(outgoing=True, pattern=r'\.info'))
        async def handler(event):
            self.update_users()
            users_len = len(self.users)
            if users_len:
                try:
                    await event.reply(info_text.format(
                        users_len,
                        self.count_of_win / users_len * 100,
                        self.count_of_now_interested,
                        self.count_of_now_answered,
                        self.count_of_now_ans_int,
                        self.count_of_now_read,
                        users_len - self.count_of_now_read,
                        self.count_of_prev_interested,
                        self.count_of_prev_answered,
                        self.count_of_prev_ans_int,
                        self.count_of_prev_read,
                        users_len - self.count_of_prev_read,
                        len(self.workers)))

                except Exception as e:
                    print(e, end=sep)

            else:
                await event.reply("data base is empty")

        @self.client.on(events.NewMessage(outgoing=True, pattern=r'\.update_bd'))
        async def handler(event):
            for bot in self.bots:
                asyncio.get_event_loop().create_task(bot.update_information())

        @self.client.on(events.NewMessage(outgoing=True, pattern=r'\.del_chat'))
        async def handler(event):
            async for message in self.clinet.iter_messages(838572639):
                await self.client.delete_messages(enttity)

        @self.client.on(events.NewMessage(outgoing=True, pattern=r'\.t'))
        async def handler(event):
            print(events.Album.Event)

        @self.client.on(events.NewMessage(outgoing=True, pattern=r'\.add_new_users'))
        async def handler(event):
            try:
                with open("files/new_usernames.txt") as file:
                    new_usernames: List[str] = list()
                    for line in file:
                        new_line: str = line.strip()
                        if self.users.get(new_line) is None:
                            new_usernames.append(new_line)

                append_new_users(new_usernames, [worker[0] for worker in self.workers])
                await event.reply(f"new usernames ({len(new_usernames)}) were added to the data base")
                print(f"new usernames ({len(new_usernames)}) were added to the data base", end=sep)

            except FileNotFoundError:
                await event.reply("no such file")

            except Exception as e:
                await event.reply("some troubles")
                print(e, end=sep)

            finally:
                await event.reply("restart the program")
                sys.exit()

        @self.client.on(events.Album(chats='me', func=lambda x: x.raw_text.startswith('.test_message')))
        async def handler(event):
            text = event.raw_text[14:]
            print(type(event))
            if text:
                await self.client.send_message('me', text, file=[x.media for x in event.messages])
            else:
                await event.reply('wrong input')

        @self.client.on(events.Album(chats='me', func=lambda x: x.raw_text.startswith(r'.sending_message')))
        async def handler(event):
            text = event.raw_text[17:]
            if text:
                for bot in self.bots:
                    bot.take_message(event)
                    asyncio.get_event_loop().create_task(bot.sending_message())

                await event.reply("usernames were sent to the bots")
            else:
                await event.reply("wrong input")

        @self.client.on(events.NewMessage(outgoing=True,
                                          func=lambda x: x.raw_text.startswith(r'.sending_message') and not x.media))
        async def handler(event):
            text = event.raw_text[17:]
            if text:
                for bot in self.bots:
                    bot.take_message(event)
                    asyncio.get_event_loop().create_task(bot.sending_message())

                await event.reply("usernames were sent to the bots")
            else:
                await event.reply("wrong input")

        async with self.client:
            await self.client.run_until_disconnected()


# ==========================BOT CLASS==========================#

class Bot:

    def __init__(self, name: str, api_id: int, api_hash: str,
                 bot_users: Dict[str, Dict[str, Any]], inherit: MainClient) -> None:

        self.client: TelegramClient = TelegramClient(name, api_id, api_hash)

        self._name: str = name
        self._api_id: int = api_id
        self._api_hash: str = api_hash
        self._count_dialogs: int = 0

        self._message: Optional[events.NewMessage.Event, events.Album.Event] = None
        self.bot_users: Dict[str, Dict[str, Any]] = bot_users
        self._inherit: MainClient = inherit

    def take_message(self, event: Union[events.Album.Event, events.NewMessage.Event]) -> None:
        self._message = event

    async def update_information(self):

        async def updating():

            dialogs = (
                await self.client(GetPeerDialogsRequest(peers=list(self.bot_users.keys())))).dialogs

            for dialog, username in zip(dialogs, self.bot_users):

                user_dict: Dict[str, Any] = self.bot_users[username]
                user_dict['is_prev_interested'] = user_dict['is_now_interested']
                user_dict['is_prev_answered'] = user_dict['is_now_answered']
                user_dict['is_prev_read'] = user_dict['is_now_read']

                if dialog.unread_count > 0:
                    flag = False
                    async for message in self.client.iter_messages(username, max_id=dialog.read_outbox_max_id):
                        await message.mark_read()
                        if '+' in message.raw_text:
                            flag = True
                            break

                    if flag and dialog.unread_count > 1:
                        user_dict['is_now_interested'] = 1
                        user_dict['is_now_answered'] = 1
                        user_dict['is_now_read'] = 1

                        await self.client.send_message(username, ans.format(ME))

                    elif flag:
                        user_dict['is_now_interested'] = 1
                        user_dict['is_now_answered'] = 0
                        user_dict['is_now_read'] = 1

                    else:
                        user_dict['is_now_interested'] = 0
                        user_dict['is_now_answered'] = 1
                        user_dict['is_now_read'] = 1

                        await self.client.send_message(username, ans.format(ME))

                elif self.bot_users[username]['message_id'] <= dialog.read_outbox_max_id:
                    user_dict['is_now_interested'] = 0
                    user_dict['is_now_answered'] = 0
                    user_dict['is_now_read'] = 1

                else:
                    user_dict['is_now_interested'] = 0
                    user_dict['is_now_answered'] = 0
                    user_dict['is_now_read'] = 0

            await asyncio.sleep(randint(10, 21) / 10)

        async with self.client:
            await updating()

        print(f"{self._name} has updated information of all the users", end=sep)

        await self._inherit.get_info_from_bots(self.bot_users, "updating")

    async def sending_message(self):

        async def sending():
            for username in self.bot_users:
                try:
                    user_dict: Dict[str, Any] = self.bot_users[username]
                    user_dict['prev_message_id'] = user_dict['new_message_id']

                    if type(self._message) == events.Album.Event:
                        await self.client.send_message(username,
                                                       self._message.raw_text[17:],
                                                       file=self._message.messages)

                        user_dict['new_message_id'] = self._message.messages[0].id

                    elif type(self._message) == events.NewMessage.Event:
                        await self.client.send_message(username,
                                                       self._message.raw_text[17:])

                        user_dict['new_message_id'] = self._message.id

                    else:
                        print(self._message)

                    await asyncio.sleep(randint(9, 60) / 10)

                except errors.FloodWaitError:
                    print(f"flood error\nApi id: {self._api_id}", end=sep)
                    break

                except Exception as e:
                    print(f"api - {self._api_id} has troubles with sending to {username}: {e}", end=sep)
                    await self.client.send_message(ME, "troubles with sending")

        async with self.client:
            await sending()

        print(f"{self._api_id} has messaged all the users", end=sep)

        await self._inherit.get_info_from_bots(self.bot_users, "sending")


# ==========================STARING PROGRAM==========================#


def get_lists_for_spam(users: Dict[str, Dict[str, Any]]) -> \
        Tuple[int, List[Dict[str, Union[str, int]]], int, int, int, int, int, int, int, int]:
    count_of_win: int = 0
    count_of_prev_interested: int = 0
    count_of_now_interested: int = 0
    count_of_prev_answered: int = 0
    count_of_now_answered: int = 0
    count_of_prev_read: int = 0
    count_of_now_read: int = 0
    count_of_prev_ans_int: int = 0
    count_of_now_ans_int: int = 0

    active_users: List[Dict[str, Union[str, int]]] = list()

    for user, user_dict in users.items():
        if user_dict['is_win']:
            count_of_win += 1

        if user_dict['is_now_interested']:
            count_of_now_interested += 1
            if user_dict['is_now_answered']:
                count_of_now_ans_int += 1

        if user_dict['is_now_answered']:
            count_of_now_answered += 1

        if user_dict['is_now_read']:
            count_of_now_read += 1

        if user_dict['is_prev_interested']:
            count_of_prev_interested += 1
            if user_dict['is_prev_answered']:
                count_of_prev_ans_int += 1

        if user_dict['is_prev_answered']:
            count_of_prev_answered += 1

        count_of_prev_read += 1
        active_users.append({'user_name': user, 'client_api': user_dict['client_api']})

    return (count_of_win, active_users,
            count_of_prev_interested, count_of_now_interested,
            count_of_prev_answered, count_of_now_answered,
            count_of_prev_read, count_of_now_read,
            count_of_prev_ans_int, count_of_now_ans_int)


def append_new_users(usernames: List[str], instances_api: List[int]) -> None:
    conn_users: Optional[sqlite3.Connection] = None

    try:
        conn_users = sqlite3.connect(r"bases/users.db")
        cursor_users = conn_users.cursor()

        counter: int = 0
        instances_size: int = len(instances_api)

        if not instances_size:
            print("please, add the client at first", end=sep)
            sys.exit()

        for username in usernames:
            if counter == instances_size:
                counter = 0

            cursor_users.execute("""INSERT INTO users
                                    (username, client_api,
                                    is_win, prev_message_id, new_message_id,
                                    is_prev_interested, is_now_interested,
                                    is_prev_answered, is_now_answered,
                                    is_prev_read, is_now_read)
                                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                """, (username, instances_api[counter],
                                      0, 0, 0,
                                      0, 0,
                                      0, 0,
                                      -1, 0))
            conn_users.commit()
            counter += 1

        cursor_users.close()

    except sqlite3.Error as e:
        print(e, end=sep)

    finally:
        if conn_users:
            conn_users.close()


def update_messages_id(update_users: Dict[str, Dict[str, Any]]) -> None:
    conn_users: Optional[sqlite3.Connection] = None

    try:
        conn_users = sqlite3.connect(r"bases/users.db")
        cursor_users = conn_users.cursor()

        for username in update_users:
            cursor_users.execute("""
                                UPDATE users
                                SET new_message_id=?
                                WHERE username=?
                                """, (update_users[username]['new_message_id'], username))
            conn_users.commit()

        cursor_users.close()

    except Exception as e:
        print(e, end=sep)

    finally:
        if conn_users:
            conn_users.close()


def update_data_base(update_users: Dict[str, Dict[str, Any]]) -> None:
    conn_users: Optional[sqlite3.Connection] = None

    try:
        conn_users = sqlite3.connect(r"bases/users.db")
        cursor_users = conn_users.cursor()

        for user, user_dict in update_users.items():
            cursor_users.execute("""
                                UPDATE users
                                SET is_win=?,
                                    prev_message_id=?,
                                    new_message_id=?,
                                    is_prev_interested=?,
                                    is_now_interested=?,
                                    is_prev_answered=?,
                                    is_now_answered=?,
                                    is_prev_read=?,
                                    is_now_read=?
                                WHERE username=?
                                """, (user_dict['is_win'],
                                      user_dict['prev_message_id'],
                                      user_dict['new_message_id'],
                                      user_dict['is_prev_interested'],
                                      user_dict['is_now_interested'],
                                      user_dict['is_prev_answered'],
                                      user_dict['is_now_answered'],
                                      user_dict['is_prev_read'],
                                      user_dict['is_now_read'],
                                      user))
            conn_users.commit()

        cursor_users.close()

    except Exception as e:
        print(e, end=sep)

    finally:
        if conn_users:
            conn_users.close()


def build_dir() -> None:
    if not os.path.isdir("bases"):
        os.mkdir("bases")

    if not os.path.isdir("files"):
        os.mkdir("files")
        open(r"files/new_usernames.txt", 'a').close()

    if not os.path.isdir("sessions"):
        os.mkdir("sessions")


def get_users_information() -> Dict[str, Dict[str, Any]]:
    # users == {username: {column_name: value, ...}, ...}
    users: Dict[str, Dict[str, Any]] = dict()
    conn_users: Optional[sqlite3.Connection] = None

    try:
        conn_users = sqlite3.connect(r"bases/users.db")
        cursor_users = conn_users.cursor()

        cursor_users.execute('''CREATE TABLE IF NOT EXISTS users(
                                username TEXT NOT NULL,
                                client_api INTEGER NOT NULL,
                                is_win INTEGER NOT NULL,
                                prev_message_id INTEGER NOT NULL,
                                new_message_id INTEGER NOT NULL,
                                is_prev_interested INTEGER NOT NULL,
                                is_now_interested INTEGER NOT NULL,
                                is_prev_answered INTEGER NOT NULL,
                                is_now_answered INTEGER NOT NULL,
                                is_prev_read INTEGER NOT NULL,
                                is_now_read INTEGER NOT NULL,
                                PRIMARY KEY (username))''')
        conn_users.commit()

        cursor_users.execute("""SELECT *
                                FROM users
                                WHERE is_prev_read!=?
                                ORDER BY is_now_interested DESC,
                                         is_now_answered DESC,
                                         is_now_read DESC""", (0,))
        conn_users.commit()

        columns = [description[0] for description in cursor_users.description]

        for user in cursor_users.fetchall():
            pairs = list(zip(columns, user))
            users.setdefault(pairs[0][1], dict(pairs[1:]))

        cursor_users.close()

    except sqlite3.Error as e:
        print(e, end=sep)

    finally:
        if conn_users:
            conn_users.close()

    return users


def get_clients_information() -> Dict[str, Dict[str, Any]]:
    # clients == {client_api: {column_name: value, ...}, ...}
    clients: Dict[str, Dict[str, Any]] = dict()
    conn_clients: Optional[sqlite3.Connection] = None

    try:
        conn_clients = sqlite3.connect(r"bases/bots.db")
        cursor_clients = conn_clients.cursor()

        cursor_clients.execute('''CREATE TABLE IF NOT EXISTS bots(
                                client_api INT NOT NULL,
                                is_prev_interested INTEGER NOT NULL,
                                is_now_interested INTEGER NOT NULL,
                                is_prev_answered INTEGER NOT NULL,
                                is_now_answered INTEGER NOT NULL,
                                is_prev_read INTEGER NOT NULL,
                                is_now_read INTEGER NOT NULL)''')
        conn_clients.commit()

        cursor_clients.execute("""SELECT *
                                FROM bots""")
        conn_clients.commit()

        columns = [description[0] for description in cursor_clients.description]

        for client in cursor_clients.fetchall():
            pairs = list(zip(columns, client))
            clients.setdefault(pairs[0][1], dict(pairs[1:]))

        cursor_clients.close()

    except sqlite3.Error as e:
        print(e, end=sep)

    finally:
        if conn_clients:
            conn_clients.close()

    return clients


def get_workers_information(users: Dict[str, Dict[str, Any]],
                            clients: Dict[str, Dict[str, Any]]) -> List[MainClient]:
    main_instances: List[Union[MainClient, Bot]] = list()

    for i, client in enumerate(main_api_id_hash):
        main_instances.append(MainClient(name=r"sessions/main_account{}".format(i),
                                         api_id=client[0],
                                         api_hash=client[1],
                                         users=users,
                                         clients=clients,
                                         workers=others_api_id_hash))

    return main_instances


def main():
    build_dir()
    users: Dict[str, Dict[str, Any]] = get_users_information()
    clients: Dict[str, Dict[str, Any]] = get_clients_information()
    workers: List[Union[MainClient, Bot]] = get_workers_information(users=users, clients=clients)
    clients_start = [worker.run() for worker in workers]

    print("PROGRAM ACTIVATE", end=sep)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.gather(*clients_start))


if __name__ == '__main__':
    main()
