import click
from datawave_cli.utilities import pods


def depends_on(option_type: click.Option):
    class DependsOn(click.Option):
        def __init__(self, *args, **kwargs):
            self.depends_on = option_type
            kwargs['help'] = (kwargs.get("help", "")
                              + f' NOTE: This parameter is dependent on `{self.depends_on}`.').strip()
            super(DependsOn, self).__init__(*args, **kwargs)

        def handle_parse_result(self, ctx, opts, args):
            dependent_exists = self.name in opts
            dependee_exists = self.depends_on in opts
            if dependent_exists and not dependee_exists:
                raise click.UsageError(f'Illegal Usage: {self.name} is dependent on {self.depends_on} being defined')
            return super(DependsOn, self).handle_parse_result(ctx, opts, args)
    return DependsOn


class File(click.Path):
    def __init__(self, *args, **kwargs):
        self.file_type = kwargs.pop('file_type', None)
        kwargs['dir_okay'] = False
        super(File, self).__init__(*args, **kwargs)

    def to_info_dict(self):
        info_dict = super().to_info_dict()
        info_dict.update(file_type=self.file_type)
        return info_dict

    def convert(self, value, param, ctx):
        if self.file_type:
            if not value.endswith(self.file_type):
                self.fail(
                    f"{value} is not of type {self.file_type}",
                    param,
                    ctx,
                )
        return super(File, self).convert(value, param, ctx)


def common_options(f):
    """A decorator for adding options common to most submodules"""
    f = click.option("-u", "--url", type=str, envvar='DWV_URL', default='', show_default=True,
                     help='The URL for the datawave DNS. If a value is passed it will use that value. '
                     + 'Otherwise, it will attempt to read the environment variable `DWV_URL`. If that variable has '
                     + 'not been set it will use the default value.')(f)
    f = click.option("-i", "--ip", is_flag=True,
                     help="Enables the usage of the IP and port instead of DNS. Both IP and port are preset based "
                     + "on namespace, this flag is mostly in case the DNS is not working. You need to have a correctly "
                     + "configured kubectl pointing to the cluster you wish to interact with as it gets the ip from "
                     + "there.")(f)
    f = click.option('-n', '--namespace', type=str, envvar='DWV_NAMESPACE', default=pods.namespace, show_default=True,
                     cls=depends_on('ip'),
                     help='The kubernetes namespace to interact with. If a value is passed it will use that value. '
                     + 'Otherwise, it will attempt to read the environment variable `DWV_NAMESPACE`. If that variable '
                     + 'has not been set it will use the default value.')(f)
    f = click.option("--log-level", default="INFO", show_default=True,
                     type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], case_sensitive=False),
                     help="The level of logging details you want displayed.")(f)
    f = click.option("-k", "--key", type=File(exists=True, file_type='.pem'), cls=depends_on('cert'),
                     help="The location of the certificate key file to use in the HTTP request.")(f)
    f = click.option("-c", "--cert", type=File(exists=True, file_type='.pem'), required=True,
                     help="The location of the certificate file to use in the HTTP request.")(f)
    return f
