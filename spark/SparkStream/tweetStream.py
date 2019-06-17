from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import socket
import json

class TweetsListener(StreamListener):

    def __init__(self, csocket):
        self.client_socket = csocket

    def on_data(self, data):
        try:
            print(data)
            self.client_socket.send(bytes(data, 'utf-8'))
        except Exception as e:
            print("send() raised exception => " + str(e))
            return False
        return True

    def on_error(self, status):
        print(status)
        return True


def sendData(c_socket):
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(key, secret)

    twitter_stream = Stream(auth, TweetsListener(c_socket))
    twitter_stream.filter(track=['india'])

def main():

    sock = socket.socket()  # Create a socket object
    host = "localhost"  # Get local machine name
    port = 5555  # Reserve a port for your service.
    sock.bind((host, port))  # Bind to the port

    sock.listen(5)  # Now wait for client connection.
    conn, addr = sock.accept()  # Establish connection with client.
    sendData(conn)


if __name__ == "__main__":
    main()
