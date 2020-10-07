# KN95 Mask Price Data

This repo contains the KN95 mask price dataset displayed on [maskprice.info](https://maskprice.info) and related tools.

## Dataset

The dataset consists of the unit prices of products advertised as KN95 masks on various online marketplaces. 
Product URLs are scraped automatically. 
Price, quantity and availability data is gathered by MTurk workers.
Pricing data for each product is gathered by multiple different workers (currently 5).

### Raw MTurk Data

The raw MTurk data can be found in a SQLite database committed into the repo: [data/db/mask_price_data.sqlite]().
SQLAlchemy ORM classes for working with the data along with comments explaining the fields are in [mp_data/db.py]().

### CSV exports

Consolidated CSV exports of the raw MTUrk data are available under [data/export]().
For each marketplace, the latest unit prices for each tracked product, and the historical median unit price time series are exported.
The contents of the CSV files are display in charts on [maskprice.info](https://maskprice.info).

Only those observations from the raw MTurk data are included in the exported CSV files where at least two of the workers tasked with retrieving data for the given product supplied the same entries.
Observations are filtered out where the absolute Z-score of the daily unit price is >= 1 within a marketplace.
The reason for the low Z-score threshold is that unit prices on the same day in the same marketplace should be relatively uniform, so a large variation probably means that it's a different product or that there was a data entry error.

### Marketplaces

The following marketplaces are tracked currently:

- [AliExpress](https://aliexpress.com)
- [Amazon USA](https://amazon.com)
- [Rakuten Germany](https://rakuten.de)

Suggestions for tracking other marketplaces are welcome in the issues.

## Dataset License

<p xmlns:dct="http://purl.org/dc/terms/" xmlns:cc="http://creativecommons.org/ns#" class="license-text">
    <a rel="cc:attributionURL" property="dct:title" href="https://maskprice.info">KN95 Mask Prices</a> by <a rel="cc:attributionURL dct:creator" property="cc:attributionName" href="https://agostbiro.com#contact">Agost Biro</a> is licensed
    under
    <a rel="license" href="https://creativecommons.org/licenses/by-nc/4.0">
        CC BY-NC 4.0.
    </a>
</p>

Pleas [get in touch](https://agostbiro.com#contact) if you wish to use the data for commercial purposes.

### Attribution

Please use the following attribution in your work using the dataset:

> [KN95 Mask Prices](https://maskprice.info) by [Agost Biro](https://agostbiro.com#contact) is licensed under [CC BY-NC 4.0](https://creativecommons.org/licenses/by-nc/4.0).

#### Markdown Attribution Snippet

```markdown
[KN95 Mask Prices](https://maskprice.info) by [Agost Biro](https://agostbiro.com#contact) 
is licensed under [CC BY-NC 4.0](https://creativecommons.org/licenses/by-nc/4.0).
```

#### LaTeX Attribution Snippet

```latex
\href{https://maskprice.info}{KN95 Mask Prices} by
\href{https://agostbiro.com\#contact}{Agost Biro} is licensed under
\href{https://creativecommons.org/licenses/by-nc/4.0}{CC BY-NC 4.0}.
```

#### HTML Attribution Snippet

```html
<p xmlns:dct="http://purl.org/dc/terms/" xmlns:cc="http://creativecommons.org/ns#" class="license-text">
    <a rel="cc:attributionURL" property="dct:title" href="https://maskprice.info">
        KN95 Mask Prices
    </a> 
    by 
    <a rel="cc:attributionURL dct:creator" property="cc:attributionName" href="https://agostbiro.com#contact">
        Agost Biro
    </a>
    is licensed under
    <a rel="license" href="https://creativecommons.org/licenses/by-nc/4.0">
        CC BY-NC 4.0
    </a>.
</p>
```
