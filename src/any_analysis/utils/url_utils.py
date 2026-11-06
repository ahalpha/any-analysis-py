import re


def url_to_name(url: str):
    url = re.sub(r"^https?:\/\/", "", url)
    url = re.sub(r"[\?#].*$", "", url)
    url = re.sub(r"[./:\\*\?\"<>|]", "_", url)
    return url
