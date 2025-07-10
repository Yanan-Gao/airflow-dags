import tableauserverclient as TSC
import functools
import threading
import concurrent.futures
import time
import logging
from datetime import datetime, timezone


class ReadWriteLock:

    def __init__(self):
        self._readers = 0
        self._read_lock = threading.Lock()
        self._write_lock = threading.Lock()

    def acquire_read(self):
        with self._read_lock:
            self._readers += 1
            if self._readers == 1:
                self._write_lock.acquire()

    def release_read(self):
        with self._read_lock:
            self._readers -= 1
            if self._readers == 0:
                self._write_lock.release()

    def acquire_write(self):
        self._write_lock.acquire()

    def release_write(self):
        self._write_lock.release()


class ServerManager(ReadWriteLock):

    # def __init__(self, username, password, site, server_endpoint, debounce_time=300):
    #     super().__init__()
    #     self.__server = None
    #     self.__username = username
    #     self.__password = password
    #     self.__site = site
    #     self.__server_endpoint = server_endpoint
    #     self.token_set_time = None
    #     self.debounce_time = debounce_time
    #     self.log_prefix = f"[SERVER MANAGER]"
    #     self.login()

    def __init__(self, token_name, token_secret, site_content_url, server_endpoint, debounce_time=300):
        super().__init__()
        self.__server = None
        self.__token_name = token_name
        self.__token_secret = token_secret
        self.__site_content_url = site_content_url
        self.__server_endpoint = server_endpoint
        self.token_set_time = None
        self.debounce_time = debounce_time
        self.log_prefix = f"[SERVER MANAGER]"
        self.login()

    def login(self):
        try:
            logging.info(f"{self.log_prefix}: Logging in to server for thread {threading.current_thread().name}")
            tableau_auth = TSC.PersonalAccessTokenAuth(self.__token_name, self.__token_secret, self.__site_content_url)
            server = TSC.Server(self.__server_endpoint, use_server_version=True)
            server.auth.sign_in(tableau_auth)
            self.__server = server
            self.token_set_time = datetime.now(timezone.utc)
            print(f"{self.log_prefix}: Server login successful for thread {threading.current_thread().name} at {self.token_set_time}")
            return self.__server
        except Exception as e:
            print(f"{self.log_prefix}: {e}")

    # def login(self):
    #     logging.info(f"{self.log_prefix}: Logging in to server for thread {threading.get_ident()}")
    #     try:
    #         tableau_auth = TSC.TableauAuth(self.__username, self.__password, self.__site)
    #         server = TSC.Server(self.__server_endpoint, use_server_version=True)
    #         server.auth.sign_in(tableau_auth)
    #         self.__server = server
    #         self.token_set_time = datetime.now(timezone.utc)
    #         logging.info(f"{self.log_prefix}: Server login successful for thread {threading.get_ident()} at {self.token_set_time}")
    #         return self.__server
    #     except Exception as e:
    #         print(f"{self.log_prefix}: {e}")

    def fetch_server(self):
        logging.info(f"{self.log_prefix}: Fetching server for {threading.current_thread().name}")
        try:
            self.acquire_read()
            return self.__server
        except Exception as e:
            print(f"{self.log_prefix}: {e}")
        finally:
            self.release_read()

    def refresh_server(self):
        try:
            logging.info(f"{self.log_prefix} - {threading.current_thread().name} - Attempting to Refresh server")
            self.acquire_write()
            if self.__server and (datetime.now(timezone.utc) - self.token_set_time).total_seconds() <= self.debounce_time:
                logging.info(
                    f"{self.log_prefix}: Debounced server refresh for {threading.current_thread().name},time until next refresh: {self.debounce_time - (datetime.now(timezone.utc) - self.token_set_time).total_seconds()} seconds"
                )
                return self.__server
            self.__server = self.login()
            return self.__server
        except Exception as e:
            print(f"{self.log_prefix}: {e}")
        finally:
            self.release_write()
