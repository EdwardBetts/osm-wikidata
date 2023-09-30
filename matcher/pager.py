"""Pagination."""

import typing
from math import ceil

from flask import Flask, request, url_for

T = typing.TypeVar("T")


class Pagination(object):
    """Pagination."""

    page: int
    per_page: int
    total_count: int

    def __init__(self, page: int, per_page: int, total_count: int) -> None:
        """Init."""
        self.page = page
        self.per_page = per_page
        self.total_count = total_count

    @property
    def pages(self) -> int:
        """Page count."""
        return int(ceil(self.total_count / float(self.per_page)))

    @property
    def has_prev(self) -> bool:
        """Has previous page."""
        return self.page > 1

    @property
    def has_next(self) -> bool:
        """Has next page."""
        return self.page < self.pages

    def slice(self, items: list[T]) -> list[T]:
        """Slice of items for the current page."""
        first = (self.page - 1) * self.per_page
        last = self.page * self.per_page
        return items[first:last]

    def iter_pages(
        self,
        left_edge: int = 2,
        left_current: int = 6,
        right_current: int = 6,
        right_edge: int = 2,
    ) -> typing.Iterator[int | None]:
        """Iterate page numbers."""
        last = 0
        for num in range(1, self.pages + 1):
            if (
                num <= left_edge
                or (
                    num > self.page - left_current - 1
                    and num < self.page + right_current
                )
                or num > self.pages - right_edge
            ):
                if last + 1 != num:
                    yield None
                yield num
                last = num


def url_for_other_page(page: int) -> str:
    """Make URL for other page."""
    assert request.view_args is not None and request.endpoint
    args = request.view_args.copy()
    args.update(request.args)
    args["page"] = page
    return url_for(request.endpoint, **args)


def init_pager(app: Flask) -> None:
    """Initialise pager."""
    app.jinja_env.globals["url_for_other_page"] = url_for_other_page
