"""DFS client CLI stub — Persona 3 will implement."""

import click
import sys
from dfs_client import DFSClient

class Context:
    def __init__(self):
        self.client = None
        self.username = None
        self.password = None

pass_context = click.make_pass_decorator(Context, ensure=True)

@click.group()
@click.option('--host', default='localhost', help='Host del NameNode')
@click.option('--port', default=8000, help='Puerto del NameNode')
@click.option('--user', 'username', required=True, help='Nombre de usuario')
@click.option('--pass', 'password', required=True, help='Contraseña')
@pass_context
def cli(ctx, host, port, username, password):
    ctx.client = DFSClient(host, port)
    ctx.username = username
    ctx.password = password

def require_login(ctx):
    try:
        ctx.client.login(ctx.username, ctx.password)
    except Exception as e:
        click.echo(f"Error de autenticacion: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.argument('local_file')
@click.argument('dfs_path')
@pass_context
def put(ctx, local_file, dfs_path):
    require_login(ctx)
    click.echo(f"Subiendo {local_file} -> {dfs_path}")


@cli.command()
@click.argument('dfs_path')
@click.argument('local_file')
@pass_context
def get(ctx, dfs_path, local_file):
    click.echo(f"Descargando {dfs_path} -> {local_file}")


@cli.command()
@click.argument('dfs_path')
@pass_context
def ls(ctx, dfs_path):
    click.echo(f"Listando {dfs_path}")


@cli.command()
@click.argument('dfs_path')
@pass_context
def rm(ctx, dfs_path):
    click.echo(f"Eliminando {dfs_path}")


@cli.command()
@click.argument('dfs_path')
@pass_context
def mkdir(ctx, dfs_path):
    click.echo(f"Creando directorio {dfs_path}")


@cli.command()
@click.argument('dfs_path')
@pass_context
def rmdir(ctx, dfs_path):
    click.echo(f"Eliminando directorio {dfs_path}")


if __name__ == '__main__':
    cli()
