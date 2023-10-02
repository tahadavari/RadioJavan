from radiojavanapi import Client

rj_client = Client()

last_id = 120198
song = rj_client.get_song_by_id(last_id)
print(song.album)

