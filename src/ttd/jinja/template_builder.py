from typing import List, Optional


class TemplateBuilder:
    """Class for building Jinja templates."""

    def __init__(self, value: str, filters: Optional[List[str]] = None):
        self.value = value
        """The value. Pass with no curly braces "{{ }}"."""

        if filters is None:
            filters = []
        self.filters = filters
        """The initial set of filters. Pass no pipe symbol "|"."""

    def with_filter(self, filter: str):
        """
        Adds a filter to transform the value.

        For available filters see the links below:
            - https://jinja.palletsprojects.com/en/2.11.x/templates/#filters
            - https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html#filters
        """
        self.filters.append(filter)
        return self

    def build(self) -> str:
        """Builds the template for using as an argument for a templated parameter."""
        return f"{{{{ {self.value}{''.join(('|' + f for f in self.filters))} }}}}"

    def __str__(self) -> str:
        return self.build()
