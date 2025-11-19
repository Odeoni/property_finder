import typer


def doctor():
    """Check your system for potential problems before running AgentQL."""
    typer.echo(
        "In the future, this command will run various checks to ensure your system is ready to run AgentQL. For now, it does nothing."
    )
