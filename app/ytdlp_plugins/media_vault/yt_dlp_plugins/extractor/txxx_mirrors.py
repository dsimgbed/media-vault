from __future__ import annotations

import re

from yt_dlp.extractor.txxx import TxxxIE


class TxxxMirrorsIE(TxxxIE):
    IE_NAME = "txxx:mirrors"
    _DOMAINS = (
        "txxx.me",
        "txxxporn.tube",
    )
    _VALID_URL = rf"""(?x)
        https?://(?:www\.|m\.)?(?P<host>{"|".join(map(re.escape, _DOMAINS))})/
        (?:videos?[/-]|embed/)(?P<id>\d+)(?:/(?P<display_id>[^/?#]+))?
    """
    _EMBED_REGEX = [
        rf'<iframe[^>]+?src=(["\'])(?P<url>(?:https?:)?//(?:www\.|m\.)?(?:{"|".join(map(re.escape, _DOMAINS))})/embed/[^"\']*)\1'
    ]
    _TESTS = []
