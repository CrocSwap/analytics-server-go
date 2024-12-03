# analytics-server-go

Provides auxiliary info to the Ambient frontend, like USD prices of assets and resolved ENS domains.

## Running

To compile, from the project root directory call

`go build`

After building, specify required environment variables:

 * `COINGECKO_API_KEY` - CoinGecko API key
 * `RPC_ETHEREUM` - Ethereum mainnet RPC URL
 * `RPC_SCROLL` - Scroll RPC URL

Then from the project root directory call

`./analytics-server-go`
