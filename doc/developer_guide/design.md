# Software Design

## Design Decisions

### Implementation Language

We decided to implement the virtual schema adapter in Lua for the following reasons:
* `+` Low latency
* `+` Databricks provides a REST API that can be accessed via HTTPS also from Lua, so the adapter does not need to use JDBC
* `+` Reusable base library `virtual-schema-common-lua`
* `-` Higher implementation effort than with Java

Considered alternatives:
* Java
  * `+` Databricks provides a JDBC driver which allows using existing base library `virtual-schema-common-jdbc`. This implements most of the business logic, only a thin customization layer would be required.
  * `-` High latency due to JVM startup

### Type Annotations

#### Should we use Type Annotations at all?

Lua is a dynamic language. Annotating Lua code with types has the following advantages:
* It improves developer experience, IDEs can provide type hints
* Running a type checker can help avoid runtime errors caused by incompatible types

Considered alternatives:
* Not using type annotations results in bad developer experience and increases the risk of runtime errors.

#### Use Comments as Type Annotations

Using special comments for annotating types has the following advantages:
* `+` Non-invasive, requires no additional build step
* `+` Modules can be published to LuaRocks without a problem. We could migrate all Lua projects (e.g. `virtual-schema-common-lua`) to use type annotations to allow cross-project type checking.
* `-` Comments are harder to read and write than "native" type annotations

Considered alternatives:

* Using a transpiler from a typed language to Lua (e.g. [TypeScriptToLua](https://github.com/TypeScriptToLua/TypeScriptToLua))
  * `+` Provides excellent IDE support (e.g. TypeScript)
  * `-` Increases build time complexity
  * `-` May cause incompatibilities due to different runtime behaviour, e.g. between [TypeScript and Lua](https://typescripttolua.github.io/docs/caveats)
  * `-` Type information is lost when publishing transpiled Lua code to LuaRocks
* Using a typed Lua variant (e.g. [Teal](https://github.com/teal-language/tl))
  * `+` Provides IDE support
  * `-` Requires an additional build step.
  * `-` It's unclear if publishing a Teal-based module to LuaRocks is possible.

#### LuaLS Annotations

We decided to use [Lua Language Server](https://luals.github.io/) (LuaLS) type annotations:
* `+` Provides plugins for VSCode, IntelliJ and Neovim
* `+` Allows running the type checker standalone in the CI build using flag `--check`
* `+` Generates documentation as JSON or Markdown using flag `--doc`
* `-` Standalone mode is not CLI friendly (see [LuaLS issue #2830](https://github.com/LuaLS/lua-language-server/issues/2830)):
  * Always exists with code `0` even when type checking fails.
  * Only generates report in hard-to-read JSON format.

Considered alternatives:
* [EmmyLua](https://github.com/EmmyLua) provides similar but incompatible type annotations
  * `+` Provides plugins for IntelliJ and VSCode
  * `-` Community seems to be less active than LuaLS, repository has less GitHub stars and release
  - `-` Doesn't seem to support type checking in standalone mode during CI-build
  * `+` Currently used in many other Lua projects, e.g. [virtual-schema-common-lua](https://github.com/exasol/virtual-schema-common-lua)
  + `+` A third party tool [lemmy-help](https://github.com/numToStr/lemmy-help) allows generating documentation but is inactive since 2022.
* [LuaDoc](https://keplerproject.github.io/luadoc/) generates documentation based on annotations
  * `-` Annotations are incompatible with LuaLS and EmmyLua
  * `-` No type checker
  * `+` Currently used in project [exasol-driver-lua](https://github.com/exasol/exasol-driver-lua)

### Lua HTTP Client

The VS needs to access the Databricks REST API via HTTPS. We decided to use the `request()` function from the `socket.http` module:
* `+` Available to Exasol UDFs, recommended by [Exasol documentation](https://docs.exasol.com/db/latest/database_concepts/udf_scripts/lua.htm#Auxiliarylibraries).
* `+` Allows using a custom socket factory with the `create` parameter. This allows customizing the TLS configuration if required.
* `-` Does not allow using a custom TLS certificate store, uses the hard coded store included in Exasol.
  * This should not be a problem, we expect that the Databricks API uses valid TLS certificates.

Considered alternatives: None.

### Language for Integration Tests

We write integration tests for the VS in Java:
* `+` Large set of libraries that simplify starting a local Exasol DB (`exasol-testcontainers`), managing DB objects (`test-db-builder-java`) and asserting table content (`hamcrest-resultset-matcher`)
* `+` `exec-maven-plugin` allows integrating the Lua build into the Maven build
* `+` Using Maven allows generating build scripts etc. with `project-keeper`
* `-` Mixing of two languages in a single project
