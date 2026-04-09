import io
from dataclasses import dataclass
from typing import Dict, List, Optional


def list_available_pdf_backends() -> List[str]:
    backends = []
    try:
        import pypdf  # noqa: F401

        backends.append("pypdf")
    except Exception:
        pass

    try:
        import fitz  # type: ignore # noqa: F401

        backends.append("pymupdf")
    except Exception:
        pass

    return backends


BOOK_PREFERRED_BACKENDS = {
    "gabbe_9": ["pypdf", "pymupdf"],
    "speroff_10": ["pypdf", "pymupdf"],
    "berek_17": ["pymupdf", "pypdf"],
}


def preferred_backends_for_book(book_id: Optional[str]) -> List[str]:
    preferred = BOOK_PREFERRED_BACKENDS.get(book_id or "", [])
    available = list_available_pdf_backends()
    ordered = [backend for backend in preferred if backend in available]
    ordered.extend(backend for backend in available if backend not in ordered)
    return ordered


@dataclass
class PDFDocumentAdapter:
    backend: str

    @property
    def page_count(self) -> int:
        raise NotImplementedError

    @property
    def metadata(self) -> Dict[str, Optional[str]]:
        raise NotImplementedError

    @property
    def outline_count(self) -> int:
        raise NotImplementedError

    def extract_page_text(self, page_number: int) -> str:
        raise NotImplementedError


class PyPDFDocumentAdapter(PDFDocumentAdapter):
    def __init__(self, source):
        super().__init__(backend="pypdf")
        from pypdf import PdfReader

        self._reader = PdfReader(source)

    @property
    def page_count(self) -> int:
        return len(self._reader.pages)

    @property
    def metadata(self) -> Dict[str, Optional[str]]:
        metadata = self._reader.metadata or {}
        return {
            "title": getattr(metadata, "title", None) if hasattr(metadata, "title") else metadata.get("/Title"),
            "author": getattr(metadata, "author", None) if hasattr(metadata, "author") else metadata.get("/Author"),
            "producer": getattr(metadata, "producer", None) if hasattr(metadata, "producer") else metadata.get("/Producer"),
        }

    @property
    def outline_count(self) -> int:
        try:
            outlines = self._reader.outline or []
            return len(outlines)
        except Exception:
            return 0

    def extract_page_text(self, page_number: int) -> str:
        return (self._reader.pages[page_number - 1].extract_text() or "").strip()


class PyMuPDFDocumentAdapter(PDFDocumentAdapter):
    def __init__(self, source):
        super().__init__(backend="pymupdf")
        import fitz  # type: ignore

        if hasattr(source, "read"):
            data = source.read()
            self._doc = fitz.open(stream=data, filetype="pdf")
        elif isinstance(source, (bytes, bytearray)):
            self._doc = fitz.open(stream=bytes(source), filetype="pdf")
        else:
            self._doc = fitz.open(source)

    @property
    def page_count(self) -> int:
        return len(self._doc)

    @property
    def metadata(self) -> Dict[str, Optional[str]]:
        metadata = self._doc.metadata or {}
        return {
            "title": metadata.get("title"),
            "author": metadata.get("author"),
            "producer": metadata.get("producer"),
        }

    @property
    def outline_count(self) -> int:
        try:
            toc = self._doc.get_toc(simple=True) or []
            return len(toc)
        except Exception:
            return 0

    def extract_page_text(self, page_number: int) -> str:
        return (self._doc[page_number - 1].get_text("text") or "").strip()


def open_pdf_document(source, preferred_backend: Optional[str] = None) -> PDFDocumentAdapter:
    available = list_available_pdf_backends()
    if preferred_backend:
        ordered = [preferred_backend] + [backend for backend in available if backend != preferred_backend]
    else:
        ordered = list(available)

    for backend in ordered:
        try:
            if backend == "pymupdf":
                return PyMuPDFDocumentAdapter(source)
            if backend == "pypdf":
                return PyPDFDocumentAdapter(source)
        except Exception:
            continue

    raise RuntimeError("No usable PDF extraction backend is available.")


def open_pdf_document_from_bytes(pdf_bytes: bytes, preferred_backend: Optional[str] = None) -> PDFDocumentAdapter:
    if preferred_backend == "pypdf" or (preferred_backend is None and "pypdf" in list_available_pdf_backends()):
        return open_pdf_document(io.BytesIO(pdf_bytes), preferred_backend=preferred_backend)
    return open_pdf_document(pdf_bytes, preferred_backend=preferred_backend)


def open_pdf_document_from_path(path: str, preferred_backend: Optional[str] = None) -> PDFDocumentAdapter:
    return open_pdf_document(path, preferred_backend=preferred_backend)


def open_book_pdf_document_from_bytes(book_id: str, pdf_bytes: bytes) -> PDFDocumentAdapter:
    for backend in preferred_backends_for_book(book_id):
        try:
            return open_pdf_document_from_bytes(pdf_bytes, preferred_backend=backend)
        except Exception:
            continue
    raise RuntimeError(f"No usable PDF extraction backend is available for {book_id}.")


def open_book_pdf_document_from_path(book_id: str, path: str) -> PDFDocumentAdapter:
    for backend in preferred_backends_for_book(book_id):
        try:
            return open_pdf_document_from_path(path, preferred_backend=backend)
        except Exception:
            continue
    raise RuntimeError(f"No usable PDF extraction backend is available for {book_id}.")
