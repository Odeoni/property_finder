from typing import Optional


def generate_next_page_element_prompt(
    query: Optional[str] = None,
) -> str:
    """
    Generate a prompt for the locator generator agent

    Parameters:
    -----------
    query (str): An AgentQL query in String format that describes the content to paginate over.

    Returns:
    --------
    str: Prompt for generating the next page locator.
    """

    # Get rid of unnecessary white spaces and new line characters
    if query is not None:
        query = " ".join(query.split())

    prompt = f"""
A operable element that navigate the webpage to the next page of content.
If there is no next page, return null.
{f"The content is described by the following AgentQL query: {query}." if query is not None else ""}
Follow the following steps:
1. Identify the content on the web page{" according to the query" if query is not None else ""}.
2. Identify the pagination control for the content.
3. identify the current page number.
4. identify a operable element that navigate to current page number plus 1.
"""
    # Get rid of white spaces
    prompt = prompt.replace("\n", " ")
    return prompt
