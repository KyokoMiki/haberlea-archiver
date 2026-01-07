"""ZIP compression and Baidu NetDisk upload extension for Haberlea.

This extension compresses downloaded directories into ZIP archives
and/or uploads them to Baidu NetDisk after the download is complete.
"""

import asyncio
import codecs
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

import msgspec

from haberlea.plugins.base import ExtensionBase
from haberlea.utils.models import ExtensionInformation

if TYPE_CHECKING:
    from haberlea.download_queue import DownloadJob
from haberlea.utils.utils import compress_to_zip, delete_path

logger = logging.getLogger(__name__)


# Extension settings exposed to the plugin system
extension_settings = ExtensionInformation(
    extension_type="post_download",
    settings={
        "priority": 100,
        "zip_enabled": False,
        "compression_level": 0,
        "delete_after_upload": False,
        "upload_enabled": False,
        "baidupcs_path": "BaiduPCS-Go",
        "upload_path": "",
    },
)


class ExtensionSettings(msgspec.Struct, kw_only=True):
    """Settings for ZIP compression and Baidu upload extension.

    Attributes:
        zip_enabled: Whether ZIP compression is enabled.
        compression_level: Compression level (0-9, 0=store, 9=best).
        delete_after_upload: Whether to delete archive and source files after
            successful upload.
        upload_enabled: Whether Baidu NetDisk upload is enabled.
        baidupcs_path: Path to the BaiduPCS-Go executable.
        upload_path: Remote path on Baidu NetDisk.
    """

    zip_enabled: bool = False
    compression_level: int = 0
    delete_after_upload: bool = False
    upload_enabled: bool = False
    baidupcs_path: str = "BaiduPCS-Go"
    upload_path: str = ""


class Archiver(ExtensionBase):
    """Extension for ZIP compression and Baidu NetDisk upload."""

    def __init__(self, settings: dict[str, Any]) -> None:
        """Initialize the extension.

        Args:
            settings: Extension configuration dictionary.
        """
        super().__init__(settings)
        self.settings = msgspec.convert(settings, ExtensionSettings)

    async def on_job_complete(self, job: "DownloadJob") -> None:
        """Execute ZIP compression and/or Baidu upload for a completed download.

        Args:
            job: The completed download job containing all track information.
        """
        if not job.download_path:
            logger.warning("Job has no download path: %s", job.job_id)
            return

        path = Path(job.download_path.rstrip("/\\"))

        if not path.exists():
            logger.warning("Download path does not exist: %s", path)
            return

        # Handle single file downloads (when force_album_format=false)
        is_single_file = path.is_file()

        archive_path: Path | None = None

        # Step 1: ZIP compression (if enabled, skip for single files)
        if self.settings.zip_enabled and not is_single_file:
            archive_path = path.with_suffix(".zip")
            try:
                logger.info("Creating ZIP archive: %s", archive_path)
                await asyncio.to_thread(
                    compress_to_zip,
                    [path],
                    archive_path,
                    self.settings.compression_level,
                )
                logger.info("ZIP archive created successfully: %s", archive_path)
            except OSError as e:
                logger.error("ZIP compression failed: %s", e)
                archive_path = None

        # Step 2: Upload to Baidu NetDisk (if enabled)
        upload_success = False
        if self.settings.upload_enabled:
            # Upload ZIP if created, otherwise upload original file/directory
            upload_target = archive_path if archive_path else path
            upload_success = await self._upload_to_baidu(upload_target)

        # Step 3: Delete source and archive after successful upload (if enabled)
        if self.settings.delete_after_upload and upload_success:
            # Delete source directory/file
            await delete_path(path)
            # Delete archive if created
            if archive_path and archive_path.exists():
                await delete_path(archive_path)

    async def _upload_to_baidu(self, path: Path) -> bool:
        """Upload a file or directory to Baidu NetDisk.

        Args:
            path: Path to the file or directory to upload.

        Returns:
            True if successful, False otherwise.
        """
        cmd = [
            self.settings.baidupcs_path,
            "upload",
            "--policy",
            "overwrite",
            str(path),
            self.settings.upload_path,
        ]

        try:
            self.log(f"上传到百度网盘: {path.name} -> {self.settings.upload_path}")
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )

            # Read output in chunks and split by \r or \n for real-time progress
            if proc.stdout:
                decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
                buffer = ""
                while True:
                    chunk = await proc.stdout.read(256)
                    if not chunk:
                        # Flush any remaining bytes in the decoder
                        buffer += decoder.decode(b"", final=True)
                        break
                    buffer += decoder.decode(chunk)
                    # splitlines() handles \r, \n, \r\n automatically
                    lines = buffer.splitlines(keepends=True)
                    # Keep the last incomplete line in buffer
                    if lines and not lines[-1].endswith(("\r", "\n")):
                        buffer = lines.pop()
                    else:
                        buffer = ""
                    for line in lines:
                        stripped = line.strip()
                        if stripped:
                            self.log(f"[BaiduPCS] {stripped}")
                # Output any remaining content
                if buffer.strip():
                    self.log(f"[BaiduPCS] {buffer.strip()}")

            await proc.wait()

            if proc.returncode != 0:
                self.log(f"❌ 百度网盘上传失败 (exit code: {proc.returncode})")
                return False

            self.log(f"✅ 百度网盘上传完成: {path.name}")
            return True
        except FileNotFoundError:
            self.log(f"❌ BaiduPCS-Go 未找到: {self.settings.baidupcs_path}")
            logger.error(
                "BaiduPCS-Go executable not found at: %s. "
                "Please install BaiduPCS-Go and ensure it is in PATH.",
                self.settings.baidupcs_path,
            )
            return False
