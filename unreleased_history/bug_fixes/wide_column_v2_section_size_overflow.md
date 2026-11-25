Fixed several ways that decoding a corrupt wide-column entity could have read out of bounds or allocated a huge amount of memory. Such entities are now reported as `Status::Corruption`.
