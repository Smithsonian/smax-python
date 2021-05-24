from .smax_client import SmaxClient


class SmaxTestClient(SmaxClient):

    def smax_connect_to(self, host, port=6379):
        print("CONNECTED!", host, port)

    def smax_disconnect(self):
        print("DISCONNECTED!")

    def smax_pull(self):
        print("PULLED")
