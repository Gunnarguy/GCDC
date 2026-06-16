from grandchase_meta_analyzer.explorer_app import format_progression_tracks
from grandchase_meta_analyzer.explorer_skill_details import ProgressionTrack
import pytest

def test_format_progression_tracks_empty() -> None:
    assert format_progression_tracks([]) == "-"
    assert format_progression_tracks(None) == "-"


def test_format_progression_tracks_single() -> None:
    track1 = ProgressionTrack(label="Chaser", values=["CL20", "CL25"], context="")
    assert format_progression_tracks([track1]) == "Chaser: CL20 / CL25"


def test_format_progression_tracks_multiple() -> None:
    track1 = ProgressionTrack(label="Chaser", values=["CL20"], context="")
    track2 = ProgressionTrack(label="Soul Imprint", values=["SI15", "SI20"], context="")
    assert format_progression_tracks([track1, track2]) == "Chaser: CL20; Soul Imprint: SI15 / SI20"


def test_format_progression_tracks_empty_values() -> None:
    track1 = ProgressionTrack(label="Chaser", values=[], context="")
    assert format_progression_tracks([track1]) == "Chaser: "
