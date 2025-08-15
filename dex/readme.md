v3: https://etherscan.io/address/0x772d3c95e5b44811a5bb6c95d4edffa2ed7ced18#code 
v2: https://etherscan.io/address/0x99ac8ca7087fa4a2a1fb6357269965a2014abc35#code

const POOLS = {
    "WBTC_USDC_V2": {
        address: "0x004375dff511095cc5a197a54140a24efef3a416",
        decimals0: 8,  // WBTC
        decimals1: 6,  // USDT
        fee: "0.3%",
        creationBlock: 17000000
    },
    "WBTC_USDC_V3": {
        address: "0x99ac8ca7087fa4a2a1fb6357269965a2014abc35", // WBTC/USDC 0.3%
        decimals0: 8,  // WBTC
        decimals1: 6,  // USDC
        fee: "0.3%",
        creationBlock: 12376729
    }
};

https://docs.uniswap.org/contracts/v3/reference/core/interfaces/pool/IUniswapV3PoolEvents
