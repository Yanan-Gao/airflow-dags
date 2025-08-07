"""
Jira utilities for common functionality across Jira data extraction DAGs
"""

import logging


def extract_text_from_description(description_obj):
    """
    Extract raw text from Jira description objects - just the text, no formatting.
    
    Handles Atlassian Document Format (ADF) structures and returns consolidated text.
    
    Args:
        description_obj (dict): The Jira description field object in ADF format
        
    Returns:
        str or None: Extracted plain text content or None if no content
    """
    if not description_obj or not isinstance(description_obj, dict):
        return None

    def get_text(content_item):
        """Recursively extract just the text content"""
        if not isinstance(content_item, dict):
            return ""

        text_parts = []

        # If it's a text node, just get the text
        if content_item.get('type') == 'text':
            return content_item.get('text', '')

        # For any other type, just process the content array
        if 'content' in content_item and isinstance(content_item['content'], list):
            for item in content_item['content']:
                text_parts.append(get_text(item))

        return ' '.join(text_parts)

    try:
        # Extract all text and clean up extra whitespace
        raw_text = get_text(description_obj)
        return ' '.join(raw_text.split()) if raw_text else None

    except Exception as e:
        logging.warning(f"Failed to extract text from description: {e}")
        return str(description_obj)
