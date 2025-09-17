## JNA / DuckDB-RS


### 使用非绑定

```
[package]
name = "double-number"
version = "0.1.0"
edition = "2021"

[lib]
crate-type = ["cdylib"]

[dependencies]
duckdb = { version = "1.4.0"   }

```

### linux环境下载 duckdb
`libduckdb-linux-amd64.zip`
解压缩后把 libduckdb.so.1.4 放到 `/usr/lib`
配置环境变量
`nano ~/.bashrc`

```bash
# Set DUCKDB_LIB_DIR for cargo builds
export DUCKDB_LIB_DIR="/usr/lib/"
# Add DuckDB library to the linker search path
export LD_LIBRARY_PATH="/usr/lib/:$LD_LIBRARY_PATH"
```

### maven plugin
```xml
<plugin>
                <groupId>org.questdb</groupId>
                <artifactId>rust-maven-plugin</artifactId>
                <version>1.2.0</version>
                <executions>
                    <execution>
                        <id>double-number</id>
                        <goals>
                            <goal>build</goal>
                        </goals>
                        <configuration>
                            <!--
                                Notes:
                                  * All paths are relative to ${project.basedir}
                                  * Spacing around double dashes to satisfy the XML parser.
                            -->

                            <!--
                                If you need to, you can customize the path to the Cargo command
                                Otherwise by default it will be searched for in the PATH.
                            -->
                            <!-- <cargoPath>/custom/path/to/rust/bin/cargo</cargoPath> -->

                            <!--
                                The path to the Rust crate we want to build (which will contain a Cargo.toml).
                            -->
                            <path>src/main/rust/double-number</path>

                            <!--
                                Passes `- - release` to `cargo build` to create a release build.

                                The default is to just call `cargo build` producing a debug build.
                                Many of the other `cargo build` options are also supported.
                                E.g. ` - - features` and ` - - no-default-features`.
                            -->
                            <release>true</release>

                            <!--
                                Copy the generated binaries to the "classes" directory in the build target.
                                We use this path because it will be bundled automatically into the final jar.

                                This is the path expected by JNA.
                            -->
                            <copyTo>${project.build.directory}/classes</copyTo>

                            <!--
                                Further nest copy into a child directory named through the target's platform.
                                The directory name is computed by the `io.questdb.jar.jni.OsInfo.platform()` method.
                            -->
                            <copyWithPlatformDir>true</copyWithPlatformDir>
                        </configuration>
                    </execution>
                    <execution>
                        <id>double-number-test</id>

                        <!--
                            This execution will run the `cargo test` command on the crate.
                            This is useful for running unit tests written in Rust.
                        -->
                        <goals>
                            <goal>test</goal>
                        </goals>

                        <configuration>
                            <!-- Relative path to the crate. -->
                            <path>src/main/rust/double-number</path>

                            <!-- Specify `true` to test a release (rather than debug) build. -->
                            <release>false</release>

                            <verbosity>-v</verbosity>

                            <environmentVariables>
                                <REVERSED_STR_PREFIX>Testing prefix</REVERSED_STR_PREFIX>
                            </environmentVariables>
                        </configuration>
                    </execution>
                </executions>
            </plugin>
```

### run 
``` mvn clean package```

 run DoubleNumber. Main
