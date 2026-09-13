#!/usr/bin/env python3
"""Fetch the official v2.4.0 image without access to a Docker daemon."""
import gzip
import hashlib
import io
import json
from pathlib import Path
import shutil
import tarfile
import urllib.request

REPO = "chainmakerofficial/chainmaker-vm-engine"
TAG = "v2.4.0"
ROOT = Path(__file__).resolve().parent / "artifacts" / "vm"


def main():
    ROOT.mkdir(parents=True, exist_ok=True)
    url = "https://auth.docker.io/token?service=registry.docker.io&scope=repository:%s:pull" % REPO
    token = json.load(urllib.request.urlopen(url, timeout=60))["token"]
    headers = {"Authorization": "Bearer " + token, "Accept": ", ".join([
        "application/vnd.docker.distribution.manifest.v2+json",
        "application/vnd.docker.distribution.manifest.list.v2+json",
        "application/vnd.oci.image.index.v1+json"])}

    def manifest(ref):
        req = urllib.request.Request("https://registry-1.docker.io/v2/%s/manifests/%s" % (REPO, ref), headers=headers)
        with urllib.request.urlopen(req, timeout=60) as response:
            data = response.read()
        return json.loads(data), "sha256:" + hashlib.sha256(data).hexdigest()

    meta, digest = manifest(TAG)
    if "manifests" in meta:
        selected = next(x for x in meta["manifests"] if x["platform"]["architecture"] == "amd64" and x["platform"]["os"] == "linux")
        meta, digest = manifest(selected["digest"])
        assert digest == selected["digest"]
    (ROOT / "provenance.json").write_text(json.dumps({"repository": REPO, "tag": TAG, "digest": digest, "manifest": meta}, indent=2))
    for blob in [meta["config"]] + meta["layers"]:
        path = ROOT / blob["digest"].split(":")[1]
        if not path.exists():
            req = urllib.request.Request("https://registry-1.docker.io/v2/%s/blobs/%s" % (REPO, blob["digest"]), headers=headers)
            with urllib.request.urlopen(req, timeout=180) as response, path.open("wb") as dest:
                shutil.copyfileobj(response, dest)
        h = hashlib.sha256()
        with path.open("rb") as src:
            for data in iter(lambda: src.read(1048576), b""):
                h.update(data)
        if "sha256:" + h.hexdigest() != blob["digest"]:
            raise ValueError("blob digest mismatch: " + str(path))
        print("verified", path.name, path.stat().st_size, flush=True)
    config = meta["config"]["digest"].split(":")[1]
    layers = [x["digest"].split(":")[1] for x in meta["layers"]]
    with tarfile.open(str(ROOT / "vm-engine-v2.4.0.tar.gz"), "w:gz", compresslevel=1) as archive:
        archive.add(str(ROOT / config), arcname=config + ".json")
        for name in layers:
            unpacked = ROOT / (name + ".tar")
            with gzip.open(str(ROOT / name), "rb") as src, unpacked.open("wb") as dest:
                shutil.copyfileobj(src, dest)
            archive.add(str(unpacked), arcname=name + "/layer.tar")
            unpacked.unlink()
        data = json.dumps([{"Config": config + ".json", "RepoTags": [REPO + ":" + TAG], "Layers": [x + "/layer.tar" for x in layers]}]).encode()
        info = tarfile.TarInfo("manifest.json")
        info.size = len(data)
        archive.addfile(info, io.BytesIO(data))
    print("image archive:", ROOT / "vm-engine-v2.4.0.tar.gz", flush=True)


if __name__ == "__main__":
    main()
