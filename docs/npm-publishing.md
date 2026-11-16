# Публикация npm

Пакет `qvdrs` использует пять платформенных пакетов с нативными модулями.
Версия основного пакета, всех платформенных пакетов и optionalDependencies
должна совпадать. Текущий выпуск - `0.7.2`.

| Пакет | Платформа |
|---|---|
| `qvdrs` | Основной пакет |
| `@bintocher/qvdrs-win32-x64-msvc` | Windows `x64` |
| `qvdrs-linux-x64-gnu` | Linux `x64` |
| `qvdrs-linux-arm64-gnu` | Linux ARM64 |
| `qvdrs-darwin-x64` | macOS Intel |
| `qvdrs-darwin-arm64` | macOS Apple Silicon |

## Доступ GitHub Actions

Каждому существующему пакету нужна отдельная связь Trusted Publishing:

| Поле | Значение |
|---|---|
| Provider | GitHub Actions |
| Organization or user | `bintocher` |
| Repository | `qvdrs` |
| Workflow filename | `release-npm.yml` |
| Environment | Пустое значение |
| Разрешение | Прямая публикация `npm publish` |

Workflow работает на GitHub-hosted runners и использует `id-token: write`.
Сохраненный npm-токен для этого не нужен. После создания новой связи первый
успешный выпуск через нее должен пройти в течение двух суток.

Документация: https://docs.npmjs.com/trusted-publishers/.

## Первый выпуск нового пакета

Trusted Publishing настраивается для уже существующего пакета. Первый выпуск
нового пакета требует входа владельца через npm CLI и подтверждения ключом.
Вход в npm на сайте сам по себе не авторизует командную строку.
Браузер автоматически открывать не нужно:

```text
npm login --auth-type=web --browser=false
npm whoami
npm publish --access public --browser=false
```

Первый выпуск Windows-пакета должен содержать настоящий файл
`qvdrs.win32-x64-msvc.node` и описание версии `0.7.2`.
Использовать модуль из сборки соответствующего исходного кода.
После публикации настроить связь с GitHub для этого пакета.

Проверено 2026-11-17: npm отклонил имя `qvdrs-win32-x64-msvc` с ошибкой
`Package name triggered spam detection`. Имя в собственном пространстве
`@bintocher/qvdrs-win32-x64-msvc` принято реестром при предварительной публикации.
Поэтому Windows-загрузчик и optionalDependencies используют scoped-имя.

Предварительная публикация через `npm stage publish` создает служебную запись
`0.0.0-stage`, а сама версия до подтверждения недоступна пользователям.
При снятии единственной предварительной версии в проверке 2026-11-17 исчезла
запись нового пакета вместе с созданной связью Trusted Publishing.
Такой порядок не заменяет надежный первый выпуск.

Документация: https://docs.npmjs.com/cli/v11/commands/npm-trust/#prerequisites
и https://docs.npmjs.com/staged-publishing/.

## Запуск выпуска

Обычный push коммита или тега не запускает публикацию. Общий выпуск запускается
при публикации GitHub Release. Для отдельного повторного выпуска npm запустить
`Publish to npm` вручную из нужной ветки.

Для `0.7.2` использовать `main`: тег `v0.7.2` создан до исправления исполнителей
macOS и Windows-зависимости. Новый тег и повышение версии для этого не нужны.

Перед общим следующим выпуском обновить Cargo.toml, Cargo.lock, pyproject.toml,
package.json, package-lock.json, пять npm/*/package.json и optionalDependencies.
GitHub Release также запускает публикации Python и Rust.

При повторном запуске уже опубликованные платформенные версии нужно проверить
и пропустить. Ошибки доступа или сети не означают отсутствия версии.
Основной пакет публиковать после всех платформенных пакетов.

После выпуска проверить наличие всех шести версий в реестре, затем установить
`qvdrs@0.7.2` в пустой каталог и проверить загрузку нативного модуля.

Документация: https://docs.npmjs.com/cli/v11/commands/npm-publish/
и https://docs.github.com/en/repositories/releasing-projects-on-github/managing-releases-in-a-repository.
