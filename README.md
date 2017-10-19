# Bongo

Bongo is a .net thrift client for Impala

## Quickstart

Add the `Bongo` package to your project:

```bash
dotnet add package Bongo
```

Then use it to create and use an Impala database:

```csharp
using (var bongoClient = new BongoClient(new IPEndPoint("localhost", 21000)))
{
    await bongoClient.CreateDatabase("test_database");
    await bongoClient.UseDatabase("test_database");
}
```

## Building

[![Travis](https://img.shields.io/travis/syncromatics/Bongo.svg)](https://travis-ci.org/syncromatics/Bongo)
[![NuGet](https://img.shields.io/nuget/v/Bongo.svg)](https://www.nuget.org/packages/Bongo/)
[![NuGet Pre Release](https://img.shields.io/nuget/vpre/Bongo.svg)](https://www.nuget.org/packages/Bongo/)

## Code of Conduct

We are committed to fostering an open and welcoming environment. Please read our [code of conduct](CODE_OF_CONDUCT.md) before participating in or contributing to this project.

## Contributing

We welcome contributions and collaboration on this project. Please read our [contributor's guide](CONTRIBUTING.md) to understand how best to work with us.

## License and Authors

[![Syncromatics Engineering logo](https://en.gravatar.com/userimage/100017782/89bdc96d68ad4b23998e3cdabdeb6e13.png?size=16) Syncromatics Engineering](https://github.com/syncromatics)

[![license](https://img.shields.io/github/license/syncromatics/Bongo.svg)](https://github.com/syncromatics/Bongo/blob/master/LICENSE)
[![GitHub contributors](https://img.shields.io/github/contributors/syncromatics/Bongo.svg)](https://github.com/syncromatics/Bongo/graphs/contributors)

This software is made available by Syncromatics Engineering under the MIT license.
