import typer

from agentql._cli._commands import doctor_command, init_command, new_script_command

app = typer.Typer(no_args_is_help=True)
app.command()(init_command.init)
app.command()(doctor_command.doctor)
app.command()(new_script_command.new_script)


if __name__ == "__main__":
    app()
