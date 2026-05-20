import click
import sys
from dfs_client import DFSClient


@click.group()
@click.option('--host', default='localhost', help='Host del NameNode')
@click.option('--port', default=8000, help='Puerto del NameNode')
@click.option('--user', 'username', required=True, help='Nombre de usuario')
@click.option('--pass', 'password', required=True, help='Contraseña')
@click.pass_context
def cli(ctx, host, port, username, password):
    ctx.ensure_object(dict)
    ctx.obj['client'] = DFSClient(host, port)
    ctx.obj['username'] = username
    ctx.obj['password'] = password


def require_login(ctx):
    try:
        ctx.obj['client'].login(ctx.obj['username'], ctx.obj['password'])
    except Exception as e:
        click.secho(f"Error: {e}", fg='red', err=True)
        sys.exit(1)


@cli.command()
@click.argument('local_file')
@click.argument('dfs_path')
@click.pass_context
def put(ctx, local_file, dfs_path):
    require_login(ctx)
    try:
        ctx.obj['client'].upload(local_file, dfs_path)
        click.secho(f"✓ Archivo subido exitosamente", fg='green')
    except Exception as e:
        click.secho(f"✗ Error: {e}", fg='red', err=True)
        sys.exit(1)


@cli.command()
@click.argument('dfs_path')
@click.argument('local_file')
@click.pass_context
def get(ctx, dfs_path, local_file):
    require_login(ctx)
    try:
        ctx.obj['client'].download(dfs_path, local_file)
        click.secho(f"✓ Archivo descargado exitosamente", fg='green')
    except Exception as e:
        click.secho(f"✗ Error: {e}", fg='red', err=True)
        sys.exit(1)


@cli.command()
@click.argument('dfs_path')
@click.pass_context
def ls(ctx, dfs_path):
    require_login(ctx)
    try:
        entries = ctx.obj['client'].ls(dfs_path)
        if not entries:
            click.echo("(vacío)")
        else:
            for e in entries:
                click.echo(e)
    except Exception as e:
        click.secho(f"✗ Error: {e}", fg='red', err=True)
        sys.exit(1)


@cli.command()
@click.argument('dfs_path')
@click.pass_context
def rm(ctx, dfs_path):
    require_login(ctx)
    try:
        ctx.obj['client'].rm(dfs_path)
        click.secho(f"✓ Archivo eliminado", fg='green')
    except Exception as e:
        click.secho(f"✗ Error: {e}", fg='red', err=True)
        sys.exit(1)


@cli.command()
@click.argument('dfs_path')
@click.pass_context
def mkdir(ctx, dfs_path):
    require_login(ctx)
    try:
        ctx.obj['client'].mkdir(dfs_path)
        click.secho(f"✓ Directorio creado", fg='green')
    except Exception as e:
        click.secho(f"✗ Error: {e}", fg='red', err=True)
        sys.exit(1)


@cli.command()
@click.argument('dfs_path')
@click.pass_context
def rmdir(ctx, dfs_path):
    require_login(ctx)
    try:
        ctx.obj['client'].rmdir(dfs_path)
        click.secho(f"✓ Directorio vacío verificado", fg='green')
    except Exception as e:
        click.secho(f"✗ Error: {e}", fg='red', err=True)
        sys.exit(1)


if __name__ == '__main__':
    cli(obj={})