from collections import namedtuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from itertools import count
from time import sleep
from retry import retry
import spotipy

from typing import Dict, Iterable, Iterator, List, Optional
from ytmusicapi import YTMusic
from spotipy.oauth2 import SpotifyOAuth

Track = namedtuple("Track", ["title", "artist", "album"])


def track_from_dict(track_dict: dict) -> Track:
    return Track(
        title=track_dict["name"],
        artist=track_dict["artists"][0]["name"],
        album=track_dict["album"]["name"],
    )


def chunks(lst: List, n: int) -> List[List]:
    if len(lst) < n:
        return lst
    chunk_size = len(lst) // n
    chunks = []
    for i in range(0, len(lst), chunk_size):
        chunks.append(lst[i : i + chunk_size])

    return chunks


class TracksExporter:
    NUM_WORKERS = 32
    SPOTIPY_SCOPE = ["playlist-read-private", "user-library-read"]

    def __init__(self, ytmusic_auth_file: str = "headers_auth.json"):
        self.spotify = spotipy.Spotify(auth_manager=SpotifyOAuth(scope=self.SPOTIPY_SCOPE))
        self.ytmusic = YTMusic(ytmusic_auth_file)

    def list_spotify_saved_tracks(self) -> Iterator[Track]:
        limit = 50
        offset = 0
        eof = False
        while not eof:
            result = self.spotify.current_user_saved_tracks(limit=limit, offset=offset)
            items = result["items"]
            offset += len(items)
            if len(items) < limit:
                eof = True
            for _, item in enumerate(items):
                track = track_from_dict(item["track"])
                yield track

    @retry(delay=0.25, backoff=2, tries=20)
    def _video_id_for_track(self, track: Track) -> Optional[str]:
        results = self.ytmusic.search(query=f"{track.title} {track.artist}", filter="songs")
        for result in results:
            if result["videoId"]:
                return result["videoId"]

    def list_spotify_playlists(self) -> Dict[str, List[Track]]:
        limit = 50
        offset = 0
        eof = False
        playlists_dict = {}

        while not eof:
            result = self.spotify.current_user_playlists(limit=limit, offset=offset)
            items = result["items"]
            offset += len(items)
            if len(items) < limit:
                eof = True
            for _, item in enumerate(items):
                tracks = self.spotify.user_playlist_tracks(playlist_id=item["id"])["items"]
                if len(tracks) == 0:
                    continue
                playlists_dict[item["name"]] = [track_from_dict(track["track"]) for track in tracks]
        return playlists_dict

    @retry(delay=0.25, backoff=2, tries=5)
    def _create_playlist_youtube_music(self, title: str, tracks: Iterable[Track]):
        return self.ytmusic.create_playlist(
            title=title,
            description="Imported from Spotify",
            video_ids=list(self.video_ids_for_tracks(tracks)),
        )

    def create_playlists_youtube_music(self, playlists: Dict[str, List[Track]]):
        for title, tracks in playlists.items():
            self._create_playlist_youtube_music(title=title, tracks=tracks)

    def video_ids_for_tracks(self, tracks: Iterable[Track]) -> Iterator[str]:

        tracks = [track for track in tracks]

        video_id_for_track: Dict[Track, str] = {}

        def _video_ids_for_chunk(tracks: List[Track]) -> List[str]:
            for track in tracks:
                video_id = self._video_id_for_track(track)
                video_id_for_track[track] = video_id
            return

        threads = []
        print(f"chunks {chunks(tracks, self.NUM_WORKERS)}")
        with ThreadPoolExecutor(max_workers=self.NUM_WORKERS) as executor:
            for chunk in chunks(tracks, self.NUM_WORKERS):
                threads.append(executor.submit(_video_ids_for_chunk, chunk))

            success = []

            for task in as_completed(threads):
                success.append(task.result())

        return (video_id_for_track[track] for track in tracks)

    def like_songs_youtube_music(self, video_ids: Iterable[str]):
        return [self._like_song_youtube_music(video_id) for video_id in video_ids]

    @retry(delay=0.25, backoff=2, tries=20)
    def _get_liked_songs_count(self) -> int:
        # print(f"_get_liked_songs_count {datetime.now()}")
        return self.ytmusic.get_liked_songs(limit=5)["trackCount"]

    def _like_song_youtube_music(self, video_id: str):
        changed = False
        count = 0
        max_retries = 5
        initial_track_count = self._get_liked_songs_count()
        while not changed and count < max_retries:
            try:
                self.ytmusic.rate_song(videoId=video_id, rating="LIKE")
            except Exception as exc:
                print(f"rate_song: {exc}")
            track_count = self._get_liked_songs_count()
            changed = track_count != initial_track_count
            count += 1

        print(f"LIKED {video_id}") if changed else print(f"FAILED TO LIKE {video_id}")

    def export_playlists(self):
        return self.create_playlists_youtube_music(playlists=self.list_spotify_playlists())

    def export_saved_tracks(self):
        liked_tracks = [track for track in self.list_spotify_saved_tracks()]
        liked_tracks.reverse()  # Reverse so that the most recent track is processed last
        video_ids = self.video_ids_for_tracks(tracks=liked_tracks)
        return self.like_songs_youtube_music(video_ids)


if __name__ == "__main__":
    exporter = TracksExporter()
    exporter.export_playlists()
